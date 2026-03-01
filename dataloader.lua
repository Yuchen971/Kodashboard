local DataStorage = require("datastorage")
local lfs = require("libs/libkoreader-lfs")
local logger = require("logger")
local ffiutil = require("ffi/util")

local DataLoader = {}
local DASHBOARD_CACHE_TTL_SEC = 8

local function cache_dashboard_payload(payload)
    DataLoader._dashboard_cache = {
        ts = os.time(),
        payload = payload,
    }
    return payload
end

local function build_hourly_slots()
    local slots = {}
    for hour = 0, 23 do
        slots[hour] = { sessions = 0, duration_sec = 0 }
    end
    return slots
end

local function append_hourly_series(target, slots)
    for hour = 0, 23 do
        local v = slots[hour] or { sessions = 0, duration_sec = 0 }
        table.insert(target, {
            hour = hour,
            sessions = tonumber(v.sessions) or 0,
            duration_sec = tonumber(v.duration_sec) or 0,
        })
    end
end

local function parse_date_ymd(s)
    if type(s) ~= "string" then return nil end
    local y, m, d = s:match("^(%d%d%d%d)%-(%d%d)%-(%d%d)$")
    if not y then return nil end
    return os.time({
        year = tonumber(y),
        month = tonumber(m),
        day = tonumber(d),
        hour = 12, min = 0, sec = 0,
    })
end

local function day_diff(a, b)
    if not a or not b then return nil end
    return math.floor((a - b) / 86400 + 0.5)
end

local function sort_desc_num(key)
    return function(a, b)
        return (tonumber(a[key]) or 0) > (tonumber(b[key]) or 0)
    end
end

local function sanitize_title_for_match(s)
    s = tostring(s or "")
    s = s:lower()
    -- Normalize common full-width/CJK punctuation to improve matching for Chinese titles.
    s = s:gsub("：", ":")
    s = s:gsub("，", ",")
    s = s:gsub("。", ".")
    s = s:gsub("；", ";")
    s = s:gsub("！", "!")
    s = s:gsub("？", "?")
    s = s:gsub("（", "(")
    s = s:gsub("）", ")")
    s = s:gsub("【", "[")
    s = s:gsub("】", "]")
    s = s:gsub("《", " ")
    s = s:gsub("》", " ")
    s = s:gsub("、", " ")
    s = s:gsub("　", " ")
    s = s:gsub("z%-library", " ")
    s = s:gsub("1lib%.sk", " ")
    s = s:gsub("z%-lib%.sk", " ")
    s = s:gsub("zlibrary%.sk", " ")
    s = s:gsub("%b()", " ")
    s = s:gsub("%b[]", " ")
    s = s:gsub("[\226\128\152\226\128\153\226\128\156\226\128\157]", "") -- ‘ ’ “ ”
    s = s:gsub("[\226\128\147\226\128\148]", " ") -- – —
    s = s:gsub("[_%-%.,:;!%?\"'`•]+", " ")
    s = s:gsub("%s+", " ")
    s = s:gsub("^%s+", "")
    s = s:gsub("%s+$", "")
    return s
end

local function score_book_match(book, candidate)
    local bt = sanitize_title_for_match(book.title)
    local ct = sanitize_title_for_match(candidate.title)
    if bt == "" or ct == "" then return 0 end

    local bt_compact = bt:gsub("%s+", "")
    local ct_compact = ct:gsub("%s+", "")
    local score = 0
    if bt == ct then score = score + 100 end
    if ct:find(bt, 1, true) or bt:find(ct, 1, true) then score = score + 60 end
    if bt_compact ~= "" and ct_compact ~= "" then
        if bt_compact == ct_compact then score = score + 80 end
        if ct_compact:find(bt_compact, 1, true) or bt_compact:find(ct_compact, 1, true) then
            score = score + 40
        end
    end

    local ba = sanitize_title_for_match(book.authors)
    local ca = sanitize_title_for_match(candidate.authors)
    if ba ~= "" and ca ~= "" then
        local ba_compact = ba:gsub("%s+", "")
        local ca_compact = ca:gsub("%s+", "")
        if ba == ca then score = score + 30 end
        if ca:find(ba, 1, true) or ba:find(ca, 1, true) then score = score + 15 end
        if ba_compact ~= "" and ca_compact ~= "" then
            if ba_compact == ca_compact then score = score + 20 end
            if ca_compact:find(ba_compact, 1, true) or ba_compact:find(ca_compact, 1, true) then
                score = score + 10
            end
        end
    end

    if (book.pages or 0) > 0 and (candidate.pages or 0) > 0 then
        if tonumber(book.pages) == tonumber(candidate.pages) then
            score = score + 20
        end
    end

    return score
end

local function image_ctype_from_path(path)
    local ext = tostring(path or ""):lower():match("%.([^.]+)$") or ""
    if ext == "jpg" or ext == "jpeg" then return "image/jpeg" end
    if ext == "png" then return "image/png" end
    if ext == "webp" then return "image/webp" end
    if ext == "gif" then return "image/gif" end
    return nil
end

local function slugify_cover_key(s)
    s = tostring(s or ""):lower()
    s = s:gsub("[/%\\:%*%?\"<>|]", " ")
    s = s:gsub("%s+", "-")
    s = s:gsub("%-+", "-")
    s = s:gsub("^%-+", "")
    s = s:gsub("%-+$", "")
    return s
end

local function stable_cover_hash_key(s)
    s = tostring(s or "")
    local h = 0
    for i = 1, #s do
        h = (h * 131 + s:byte(i)) % 4294967296
    end
    return string.format("f-%08x", h)
end

local function trim_string(s)
    s = tostring(s or "")
    s = s:gsub("^%s+", "")
    s = s:gsub("%s+$", "")
    return s
end

local function compute_book_ref(doc_path, md5)
    local md5v = trim_string(md5)
    if md5v ~= "" then
        return md5v, "md5"
    end
    local path_key = stable_cover_hash_key(trim_string(doc_path))
    return "path-" .. path_key, "pathhash"
end

local function normalize_book_ref(book_ref)
    local ref = trim_string(book_ref)
    if ref == "" then return nil end
    return ref
end

local function normalize_cover_query(s)
    s = tostring(s or "")
    s = s:gsub("^%s+", "")
    s = s:gsub("%s+$", "")
    s = s:gsub("%b()", " ")
    s = s:gsub("%b[]", " ")
    s = s:gsub("%b{}", " ")
    s = s:gsub("[|｜].*$", " ")
    s = s:gsub("%f[%a]novel chapters?%f[%A].*$", " ")
    s = s:gsub("%f[%a]light novel pub%f[%A].*$", " ")
    s = s:gsub("%f[%a]z%-library%f[%A].*$", " ")
    s = s:gsub("%f[%a]zlibrary%f[%A].*$", " ")
    s = s:gsub("%f[%a]1lib%.sk%f[%A].*$", " ")
    s = s:gsub("%f[%a]z%-lib%.sk%f[%A].*$", " ")
    s = s:gsub("[%-%._]+", " ")
    s = s:gsub("%s+", " ")
    s = s:gsub("^%s+", "")
    s = s:gsub("%s+$", "")
    return s
end

local function add_unique_path(paths, path)
    if not path or path == "" then return end
    for _, existing in ipairs(paths) do
        if existing == path then return end
    end
    table.insert(paths, path)
end

local function find_existing_path(paths)
    for _, p in ipairs(paths) do
        if lfs.attributes(p, "mode") == "file" or lfs.attributes(p, "mode") == "directory" then
            return p
        end
    end
    return nil
end

local function each_parent_dir(start_dir, fn, max_depth)
    local dir = start_dir
    local depth = 0
    local limit = max_depth or 8
    while dir and dir ~= "" and depth < limit do
        fn(dir)
        local parent = dir:match("^(.*)/[^/]+$")
        if not parent or parent == dir then break end
        dir = parent
        depth = depth + 1
    end
end

local function resolve_doc_path(doc_path)
    if not doc_path or doc_path == "" then return doc_path end
    if lfs.attributes(doc_path, "mode") then return doc_path end

    local suffix = doc_path:match("^/mnt/[^/]+/documents(.*)$")
    if suffix then
        local cwd = lfs.currentdir() or ""
        local candidates = {}

        each_parent_dir(cwd, function(dir)
            add_unique_path(candidates, dir .. "/documents" .. suffix)
            add_unique_path(candidates, dir .. "/koreader/documents" .. suffix)
        end, 10)

        local resolved = find_existing_path(candidates)
        if resolved then return resolved end
    end
    return doc_path
end

local function resolve_statistics_db_path()
    local candidates = {}

    local settings_dir = DataStorage:getSettingsDir()
    if settings_dir and settings_dir ~= "" then
        add_unique_path(candidates, settings_dir .. "/statistics.sqlite3")
    end

    local data_dir = DataStorage:getDataDir()
    if data_dir and data_dir ~= "" then
        add_unique_path(candidates, data_dir .. "/statistics.sqlite3")
        add_unique_path(candidates, data_dir .. "/settings/statistics.sqlite3")
    end

    local cwd = lfs.currentdir() or ""
    each_parent_dir(cwd, function(dir)
        add_unique_path(candidates, dir .. "/settings/statistics.sqlite3")
        add_unique_path(candidates, dir .. "/koreader/settings/statistics.sqlite3")
        add_unique_path(candidates, dir .. "/statistics.sqlite3")
    end, 10)

    return find_existing_path(candidates) or (candidates[1] or "statistics.sqlite3")
end

local function new_dashboard_payload()
    return {
        summary = {
            total_books = 0,
            reading_books = 0,
            finished_books = 0,
            total_read_time_sec = 0,
            total_read_pages = 0,
            total_highlights = 0,
            total_notes = 0,
            active_days_90d = 0,
            best_streak_days = 0,
            current_streak_days = 0,
            last_read_date = "",
        },
        kpis = {
            last_7_days_time_sec = 0,
            last_30_days_time_sec = 0,
            avg_daily_time_30d_sec = 0,
            longest_day_sec = 0,
            books_touched_30d = 0,
            books_touched_90d = 0,
            books_touched_180d = 0,
            books_touched_365d = 0,
        },
        series = {
            daily_90d = {},
            daily_180d = {},
            daily_365d = {},
            monthly_12m = {},
            weekday_avg = {},
            hourly_activity = {},
            hourly_activity_30d = {},
            hourly_activity_90d = {},
            hourly_activity_180d = {},
            hourly_activity_365d = {},
        },
        calendar = {
            days = {},
            legend = { max_daily_sec_90d = 0 },
        },
        top_books = {
            by_time = {},
            by_pages = {},
            by_time_30d = {},
            by_time_90d = {},
            by_time_180d = {},
            by_time_365d = {},
            by_pages_30d = {},
            by_pages_90d = {},
            by_pages_180d = {},
            by_pages_365d = {},
        },
    }
end

function DataLoader:getHistoryItems()
    local history_file = ffiutil.joinPath(DataStorage:getDataDir(), "history.lua")
    local ok, history = pcall(dofile, history_file)
    if not ok or type(history) ~= "table" then
        logger.warn("KoDashboard: Failed to load history.lua")
        return {}
    end
    return history
end

function DataLoader:findHistoryItemByRef(book_ref)
    local want = normalize_book_ref(book_ref)
    if not want then return nil end
    local history = self:getHistoryItems()
    for i, item in ipairs(history) do
        if item and item.file then
            local sdr_data = self:loadSidecar(item.file)
            local md5 = sdr_data and sdr_data.partial_md5_checksum or nil
            local ref, id_type = compute_book_ref(item.file, md5)
            if ref == want then
                return item, i, sdr_data, ref, id_type
            end
        end
    end
    return nil
end

function DataLoader:getBooks()
    local history = self:getHistoryItems()
    local books = {}
    for i, item in ipairs(history) do
        local doc_path = item.file
        local resolved_doc_path = resolve_doc_path(doc_path)
        if lfs.attributes(resolved_doc_path, "mode") ~= "file" then
            -- Skip stale history entries whose source file no longer exists.
            goto continue
        end
        local sdr_data = self:loadSidecar(doc_path)
        local title = doc_path:match("([^/]+)%.[^.]+$") or doc_path
        local authors = ""
        local doc_pages = 0
        local percent = 0
        local language = ""
        local status = "reading"
        local last_open = ""
        local highlights_count = 0
        local notes_count = 0
        local md5 = nil

        if sdr_data then
            if sdr_data.doc_props then
                title = sdr_data.doc_props.title or title
                authors = sdr_data.doc_props.authors or ""
                language = sdr_data.doc_props.language or ""
            end
            md5 = sdr_data.partial_md5_checksum
            doc_pages = sdr_data.doc_pages or 0
            percent = sdr_data.percent_finished or 0
            if sdr_data.summary then
                status = sdr_data.summary.status or "reading"
                last_open = sdr_data.summary.modified or ""
            end

            local ann_count = 0
            local note_count = 0
            if sdr_data.annotations then
                for _, ann in ipairs(sdr_data.annotations) do
                    if ann.text and ann.text ~= "" then
                        ann_count = ann_count + 1
                    end
                    if ann.note then
                        note_count = note_count + 1
                    end
                end
            end
            highlights_count = ann_count
            notes_count = note_count
        end

        local book_ref, id_type = compute_book_ref(doc_path, md5)
        local cover_info = self:getCoverInfoForPath(doc_path, sdr_data)

        table.insert(books, {
            id = book_ref,
            legacy_index = i,
            id_type = id_type,
            file = doc_path,
            title = title,
            authors = authors,
            language = language,
            pages = doc_pages,
            percent = math.floor(percent * 1000) / 10,
            status = status,
            last_open = last_open,
            last_open_ts = item.time,
            highlights = highlights_count,
            notes = notes_count,
            md5 = md5,
            cover_available = cover_info ~= nil,
            cover_content_type = cover_info and cover_info.content_type or nil,
        })
        ::continue::
    end
    return books
end

function DataLoader:getAnnotations(book_ref)
    local item, _, sdr_data = self:findHistoryItemByRef(book_ref)
    if not item then return nil end
    if not sdr_data then return {} end

    local raw = sdr_data.annotations or {}
    local annotations = {}
    for _, ann in ipairs(raw) do
        table.insert(annotations, {
            text = ann.text or "",
            note = ann.note,
            chapter = ann.chapter or "",
            page = ann.page,
            pageno = ann.pageno,
            pos0 = ann.pos0,
            pos1 = ann.pos1,
            datetime = ann.datetime or "",
            datetime_updated = ann.datetime_updated,
            color = ann.color or "yellow",
            drawer = ann.drawer or "lighten",
            total_pages = ann.total_pages or sdr_data.doc_pages,
        })
    end
    return annotations, sdr_data.doc_props
end

function DataLoader:getBookTimeline(book_ref)
    local books = self:getBooks()
    local book = nil
    for _, b in ipairs(books) do
        if b and tostring(b.id) == tostring(book_ref) then
            book = b
            break
        end
    end
    if not book then return nil end

    local db_path = resolve_statistics_db_path()
    if lfs.attributes(db_path, "mode") ~= "file" then
        return {
            book_id = book.id,
            book_ref = book.id,
            title = book.title,
            authors = book.authors,
            stats_book_id = nil,
            sessions = {},
            total = 0,
        }
    end

    local load_ok, SQ3 = pcall(require, "lua-ljsqlite3/init")
    if not load_ok then
        logger.warn("KoDashboard: Failed to load lua-ljsqlite3:", SQ3)
        return {
            book_id = book.id,
            book_ref = book.id,
            title = book.title,
            authors = book.authors,
            stats_book_id = nil,
            sessions = {},
            total = 0,
        }
    end

    local open_ok, conn = pcall(SQ3.open, db_path)
    if not open_ok then
        logger.warn("KoDashboard: Failed to open statistics db:", conn)
        return {
            book_id = book.id,
            book_ref = book.id,
            title = book.title,
            authors = book.authors,
            stats_book_id = nil,
            sessions = {},
            total = 0,
        }
    end

    local has_book = false
    local has_page_stat = false
    pcall(function()
        local c1 = conn:rowexec("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='book'")
        has_book = c1 and tonumber(c1) > 0
        local c2 = conn:rowexec("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='page_stat_data'")
        has_page_stat = c2 and tonumber(c2) > 0
    end)

    if not has_book or not has_page_stat then
        conn:close()
        return {
            book_id = book.id,
            book_ref = book.id,
            title = book.title,
            authors = book.authors,
            stats_book_id = nil,
            sessions = {},
            total = 0,
        }
    end

    local matched = nil
    local best_score = -1

    if book.md5 and tostring(book.md5) ~= "" then
        pcall(function()
            local stmt = conn:prepare([[
                SELECT id, title, authors, pages, last_open
                FROM book
                WHERE md5 = ?
                LIMIT 1
            ]])
            local row = stmt:reset():bind(tostring(book.md5)):step()
            if row then
                matched = {
                    id = tonumber(row[1]) or 0,
                    title = row[2] and tostring(row[2]) or "",
                    authors = row[3] and tostring(row[3]) or "",
                    pages = tonumber(row[4]) or 0,
                    last_open = tonumber(row[5]) or 0,
                }
                best_score = 999
            end
            stmt:close()
        end)
    end

    if not matched then pcall(function()
        local stmt = conn:prepare([[
            SELECT id, title, authors, pages, last_open
            FROM book
            ORDER BY last_open DESC
        ]])
        local row = stmt:step()
        while row do
            local candidate = {
                id = tonumber(row[1]) or 0,
                title = row[2] and tostring(row[2]) or "",
                authors = row[3] and tostring(row[3]) or "",
                pages = tonumber(row[4]) or 0,
                last_open = tonumber(row[5]) or 0,
            }
            local score = score_book_match(book, candidate)
            if score > best_score then
                best_score = score
                matched = candidate
            end
            row = stmt:step()
        end
        stmt:close()
    end) end

    if not matched or best_score < 40 then
        conn:close()
        return {
            book_id = book.id,
            book_ref = book.id,
            title = book.title,
            authors = book.authors,
            stats_book_id = nil,
            sessions = {},
            total = 0,
        }
    end

    local sessions = {}
    local daily = {}
    local total_sessions = 0
    local first_session = nil
    local last_session = nil
    local function row_to_session(row)
        local start_ts = tonumber(row[2]) or 0
        return {
            page = tonumber(row[1]) or 0,
            start_time = start_ts,
            duration = tonumber(row[3]) or 0,
            total_pages = tonumber(row[4]) or 0,
            date = start_ts > 0 and os.date("%Y-%m-%d", start_ts) or "",
            time = start_ts > 0 and os.date("%H:%M", start_ts) or "",
        }
    end

    pcall(function()
        local stmt = conn:prepare([[
            SELECT COUNT(*)
            FROM page_stat_data
            WHERE id_book = ?
              AND start_time > 0
        ]])
        local row = stmt:reset():bind(matched.id):step()
        if row then
            total_sessions = tonumber(row[1]) or 0
        end
        stmt:close()
    end)

    pcall(function()
        local stmt = conn:prepare([[
            SELECT page, start_time, duration, total_pages
            FROM page_stat_data
            WHERE id_book = ?
              AND start_time > 0
            ORDER BY start_time ASC
            LIMIT 1
        ]])
        local row = stmt:reset():bind(matched.id):step()
        if row then first_session = row_to_session(row) end
        stmt:close()
    end)

    pcall(function()
        local stmt = conn:prepare([[
            SELECT page, start_time, duration, total_pages
            FROM page_stat_data
            WHERE id_book = ?
              AND start_time > 0
            ORDER BY start_time DESC
            LIMIT 1
        ]])
        local row = stmt:reset():bind(matched.id):step()
        if row then last_session = row_to_session(row) end
        stmt:close()
    end)

    pcall(function()
        local stmt = conn:prepare([[
            SELECT page, start_time, duration, total_pages
            FROM page_stat_data
            WHERE id_book = ?
              AND start_time > 0
            ORDER BY start_time DESC
            LIMIT 200
        ]])
        local row = stmt:reset():bind(matched.id):step()
        while row do
            table.insert(sessions, row_to_session(row))
            row = stmt:step()
        end
        stmt:close()
    end)

    pcall(function()
        local stmt = conn:prepare([[
            SELECT date(start_time, 'unixepoch', 'localtime') as day,
                   SUM(duration) as total_duration,
                   COUNT(*) as session_count,
                   COUNT(DISTINCT page) as pages_touched
            FROM page_stat_data
            WHERE id_book = ?
              AND start_time > 0
            GROUP BY day
            ORDER BY day DESC
            LIMIT 420
        ]])
        local row = stmt:reset():bind(matched.id):step()
        while row do
            table.insert(daily, {
                date = row[1] and tostring(row[1]) or "",
                duration_sec = tonumber(row[2]) or 0,
                sessions = tonumber(row[3]) or 0,
                pages = tonumber(row[4]) or 0,
            })
            row = stmt:step()
        end
        stmt:close()
    end)

    conn:close()
    return {
        book_id = book.id,
        book_ref = book.id,
        title = book.title,
        authors = book.authors,
        stats_book_id = matched.id,
        matched_title = matched.title,
        matched_authors = matched.authors,
        sessions = sessions,
        first_session = first_session,
        last_session = last_session,
        daily = daily,
        total = total_sessions > 0 and total_sessions or #sessions,
    }
end

function DataLoader:getCoverInfoForPath(doc_path, sdr_data)
    if not doc_path then return nil end
    doc_path = resolve_doc_path(doc_path)
    local candidates = {}

    local function add_candidate(p)
        if not p or p == "" then return end
        for _, c in ipairs(candidates) do
            if c == p then return end
        end
        table.insert(candidates, p)
    end

    sdr_data = sdr_data or self:loadSidecar(doc_path)

    -- Independent cover storage: <KOReader data>/kodashboard/covers/
    -- Naming priority: md5.* -> title--authors.* -> filename.*
    local data_dir = DataStorage:getDataDir()
    local cover_dir = data_dir and (data_dir .. "/kodashboard/covers") or nil
    if cover_dir and lfs.attributes(cover_dir, "mode") == "directory" then
        local filename = doc_path:match("([^/]+)$") or ""
        local stem = filename:match("(.+)%.[^.]+$") or filename
        local keys = {}
        local md5_key = nil

        if sdr_data and sdr_data.partial_md5_checksum and sdr_data.partial_md5_checksum ~= "" then
            md5_key = tostring(sdr_data.partial_md5_checksum)
            table.insert(keys, md5_key)
        end

        if sdr_data and sdr_data.doc_props then
            local raw_title = sdr_data.doc_props.title or ""
            local raw_authors = sdr_data.doc_props.authors or ""
            local title_key = slugify_cover_key(raw_title)
            local authors_key = slugify_cover_key(raw_authors)
            local norm_title_key = slugify_cover_key(normalize_cover_query(raw_title))
            local norm_authors_key = slugify_cover_key(normalize_cover_query(raw_authors))

            if title_key ~= "" then
                table.insert(keys, title_key)
                if authors_key ~= "" then
                    table.insert(keys, title_key .. "--" .. authors_key)
                end
            end
            if norm_title_key ~= "" then
                table.insert(keys, norm_title_key)
                if norm_authors_key ~= "" then
                    table.insert(keys, norm_title_key .. "--" .. norm_authors_key)
                end
            end
        end

        local stem_key = slugify_cover_key(stem)
        if stem_key ~= "" then table.insert(keys, stem_key) end
        local norm_stem_key = slugify_cover_key(normalize_cover_query(stem))
        if norm_stem_key ~= "" then table.insert(keys, norm_stem_key) end
        if stem ~= "" then table.insert(keys, stable_cover_hash_key(stem)) end

        -- KoInsight-style lookup: any file that starts with md5
        if md5_key then
            pcall(function()
                for entry in lfs.dir(cover_dir) do
                    if entry ~= "." and entry ~= ".." and entry:sub(1, #md5_key) == md5_key then
                        add_candidate(cover_dir .. "/" .. entry)
                    end
                end
            end)
        end

        for _, key in ipairs(keys) do
            add_candidate(cover_dir .. "/" .. key .. ".jpg")
            add_candidate(cover_dir .. "/" .. key .. ".jpeg")
            add_candidate(cover_dir .. "/" .. key .. ".png")
            add_candidate(cover_dir .. "/" .. key .. ".webp")
        end
    end

    local sidecar_dirs = self:getSidecarCandidates(doc_path)
    for _, sdr_dir in ipairs(sidecar_dirs) do
        if lfs.attributes(sdr_dir, "mode") == "directory" then
            add_candidate(sdr_dir .. "/cover.jpg")
            add_candidate(sdr_dir .. "/cover.jpeg")
            add_candidate(sdr_dir .. "/cover.png")
            add_candidate(sdr_dir .. "/custom_cover.jpg")
            add_candidate(sdr_dir .. "/custom_cover.png")
            pcall(function()
                for entry in lfs.dir(sdr_dir) do
                    if entry ~= "." and entry ~= ".." then
                        local lower = entry:lower()
                        if lower:match("^cover.*%.jpe?g$") or lower:match("^cover.*%.png$") or lower:match("^cover.*%.webp$") then
                            add_candidate(sdr_dir .. "/" .. entry)
                        end
                    end
                end
            end)
        end
    end

    local dir = doc_path:match("^(.+)/[^/]+$") or ""
    local filename = doc_path:match("([^/]+)$") or ""
    local stem = filename:match("(.+)%.[^.]+$") or filename
    if dir ~= "" then
        add_candidate(dir .. "/" .. stem .. ".jpg")
        add_candidate(dir .. "/" .. stem .. ".jpeg")
        add_candidate(dir .. "/" .. stem .. ".png")
        add_candidate(dir .. "/" .. stem .. ".webp")
        add_candidate(dir .. "/cover.jpg")
        add_candidate(dir .. "/cover.png")
    end

    for _, p in ipairs(candidates) do
        if lfs.attributes(p, "mode") == "file" then
            local ctype = image_ctype_from_path(p)
            if ctype then
                return { path = p, content_type = ctype }
            end
        end
    end
    return nil
end

function DataLoader:getBookCover(book_ref)
    local item, _, sdr_data = self:findHistoryItemByRef(book_ref)
    if not item or not item.file then return nil end
    return self:getCoverInfoForPath(item.file, sdr_data)
end

function DataLoader:getAllHighlights()
    local history = self:getHistoryItems()
    local all = {}
    for i, item in ipairs(history) do
        local resolved_doc_path = resolve_doc_path(item.file)
        if lfs.attributes(resolved_doc_path, "mode") ~= "file" then
            goto continue
        end
        local sdr_data = self:loadSidecar(item.file)
        if sdr_data and sdr_data.annotations then
            local title = "Unknown"
            local authors = ""
            if sdr_data.doc_props then
                title = sdr_data.doc_props.title or title
                authors = sdr_data.doc_props.authors or ""
            end
            local book_md5 = sdr_data.partial_md5_checksum or ""
            local book_ref, _ = compute_book_ref(item.file, book_md5)
            for _, ann in ipairs(sdr_data.annotations) do
                if ann.text and ann.text ~= "" then
                    table.insert(all, {
                        book_id = i,
                        book_ref = book_ref,
                        book_md5 = book_md5,
                        book_title = title,
                        book_authors = authors,
                        text = ann.text,
                        note = ann.note,
                        chapter = ann.chapter or "",
                        page = ann.page,
                        pageno = ann.pageno,
                        pos0 = ann.pos0,
                        pos1 = ann.pos1,
                        datetime = ann.datetime or "",
                        datetime_updated = ann.datetime_updated,
                        color = ann.color or "yellow",
                        drawer = ann.drawer or "lighten",
                        total_pages = ann.total_pages or sdr_data.doc_pages,
                    })
                end
            end
        end
        ::continue::
    end
    return all
end

function DataLoader:getStats()
    local db_path = resolve_statistics_db_path()
    if lfs.attributes(db_path, "mode") ~= "file" then
        return { books = {}, daily = {} }
    end

    local load_ok, SQ3 = pcall(require, "lua-ljsqlite3/init")
    if not load_ok then
        logger.warn("KoDashboard: Failed to load lua-ljsqlite3:", SQ3)
        return { books = {}, daily = {} }
    end

    local open_ok, conn = pcall(SQ3.open, db_path)
    if not open_ok then
        logger.warn("KoDashboard: Failed to open statistics db:", conn)
        return { books = {}, daily = {} }
    end

    local book_stats = {}
    local has_book = false
    pcall(function()
        local check = conn:rowexec("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='book'")
        has_book = check and tonumber(check) > 0
    end)

    if has_book then
        pcall(function()
            local stmt = conn:prepare("SELECT id, title, authors, total_read_time, total_read_pages, pages, last_open, md5 FROM book ORDER BY last_open DESC")
            local row = stmt:step()
            while row do
                table.insert(book_stats, {
                    id = tonumber(row[1]) or 0,
                    title = row[2] and tostring(row[2]) or "",
                    authors = row[3] and tostring(row[3]) or "",
                    total_read_time = tonumber(row[4]) or 0,
                    total_read_pages = tonumber(row[5]) or 0,
                    pages = tonumber(row[6]) or 0,
                    last_open = tonumber(row[7]) or 0,
                    md5 = row[8] and tostring(row[8]) or "",
                })
                row = stmt:step()
            end
            stmt:close()
        end)
    end

    local daily_stats = {}
    local has_page_stat = false
    pcall(function()
        local check = conn:rowexec("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='page_stat_data'")
        has_page_stat = check and tonumber(check) > 0
    end)

    if has_page_stat then
        pcall(function()
            local stmt = conn:prepare([[
                SELECT date(start_time, 'unixepoch', 'localtime') as day,
                       SUM(duration) as total_duration,
                       COUNT(DISTINCT id_book) as books_count
                FROM page_stat_data
                WHERE start_time > 0
                GROUP BY day
                ORDER BY day DESC
                LIMIT 90
            ]])
            local row = stmt:step()
            while row do
                table.insert(daily_stats, {
                    date = row[1] and tostring(row[1]) or "",
                    duration = tonumber(row[2]) or 0,
                    books = tonumber(row[3]) or 0,
                })
                row = stmt:step()
            end
            stmt:close()
        end)
    end

    conn:close()
    return { books = book_stats, daily = daily_stats }
end

function DataLoader:getDashboard()
    local cache = self._dashboard_cache
    local now_ts = os.time()
    if cache and cache.payload and (now_ts - (tonumber(cache.ts) or 0) <= DASHBOARD_CACHE_TTL_SEC) then
        return cache.payload
    end

    local payload = new_dashboard_payload()

    local books = self:getBooks()
    local summary = payload.summary
    summary.total_books = #books
    for _, b in ipairs(books) do
        if b.status == "complete" or b.status == "finished" then
            summary.finished_books = summary.finished_books + 1
        elseif (b.percent or 0) > 0 then
            summary.reading_books = summary.reading_books + 1
        end
        summary.total_highlights = summary.total_highlights + (b.highlights or 0)
        summary.total_notes = summary.total_notes + (b.notes or 0)
    end

    local db_path = resolve_statistics_db_path()
    if lfs.attributes(db_path, "mode") ~= "file" then
        return cache_dashboard_payload(payload)
    end

    local load_ok, SQ3 = pcall(require, "lua-ljsqlite3/init")
    if not load_ok then
        logger.warn("KoDashboard: Failed to load lua-ljsqlite3:", SQ3)
        return cache_dashboard_payload(payload)
    end

    local open_ok, conn = pcall(SQ3.open, db_path)
    if not open_ok then
        logger.warn("KoDashboard: Failed to open statistics db:", conn)
        return cache_dashboard_payload(payload)
    end

    local has_book = false
    local has_page_stat = false
    pcall(function()
        local c1 = conn:rowexec("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='book'")
        has_book = c1 and tonumber(c1) > 0
        local c2 = conn:rowexec("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='page_stat_data'")
        has_page_stat = c2 and tonumber(c2) > 0
    end)

    local stats_books = {}
    local stats_book_map = {}

    if has_book then
        pcall(function()
            local stmt = conn:prepare([[
                SELECT id, title, authors, total_read_time, total_read_pages, pages, last_open, md5
                FROM book
                ORDER BY last_open DESC
            ]])
            local row = stmt:step()
            while row do
                local item = {
                    stats_book_id = tonumber(row[1]) or 0,
                    title = row[2] and tostring(row[2]) or "",
                    authors = row[3] and tostring(row[3]) or "",
                    total_read_time_sec = tonumber(row[4]) or 0,
                    total_read_pages = tonumber(row[5]) or 0,
                    pages = tonumber(row[6]) or 0,
                    last_open_ts = tonumber(row[7]) or 0,
                    md5 = row[8] and tostring(row[8]) or "",
                }
                table.insert(stats_books, item)
                stats_book_map[item.stats_book_id] = item
                row = stmt:step()
            end
            stmt:close()
        end)
    end

    for _, sb in ipairs(stats_books) do
        summary.total_read_time_sec = summary.total_read_time_sec + (sb.total_read_time_sec or 0)
        summary.total_read_pages = summary.total_read_pages + (sb.total_read_pages or 0)
    end

    if not has_page_stat then
        table.sort(stats_books, sort_desc_num("total_read_time_sec"))
        for i = 1, math.min(8, #stats_books) do
            table.insert(payload.top_books.by_time, stats_books[i])
        end
        table.sort(stats_books, sort_desc_num("total_read_pages"))
        for i = 1, math.min(8, #stats_books) do
            table.insert(payload.top_books.by_pages, stats_books[i])
        end
        conn:close()
        return cache_dashboard_payload(payload)
    end

    local daily_all = {}
    local daily_90 = {}
    local daily_map_180 = {}
    local day_book_map_30 = {}
    local day_book_map_90 = {}
    local day_book_map_180 = {}
    local day_book_map_365 = {}
    local top_rollup_30 = {}
    local top_rollup_90 = {}
    local top_rollup_180 = {}
    local top_rollup_365 = {}

    local function accumulate_top_rollup(store, book_id, duration_sec, pages_events)
        if not store or not book_id or book_id == 0 then return end
        local item = store[book_id]
        if not item then
            local sb = stats_book_map[book_id] or {}
            item = {
                stats_book_id = book_id,
                title = sb.title or "",
                authors = sb.authors or "",
                md5 = sb.md5 or "",
                total_read_time_sec = 0,
                total_read_pages = 0,
            }
            store[book_id] = item
        end
        item.total_read_time_sec = (item.total_read_time_sec or 0) + (tonumber(duration_sec) or 0)
        item.total_read_pages = (item.total_read_pages or 0) + (tonumber(pages_events) or 0)
    end

    pcall(function()
        local stmt = conn:prepare([[
            SELECT date(start_time, 'unixepoch', 'localtime') as day,
                   SUM(duration) as total_duration,
                   COUNT(DISTINCT id_book) as books_count,
                   COUNT(*) as pages_events
            FROM page_stat_data
            WHERE start_time > 0
            GROUP BY day
            ORDER BY day ASC
        ]])
        local row = stmt:step()
        while row do
            local item = {
                date = row[1] and tostring(row[1]) or "",
                duration_sec = tonumber(row[2]) or 0,
                books_count = tonumber(row[3]) or 0,
                pages_events = tonumber(row[4]) or 0,
            }
            if item.date ~= "" then
                table.insert(daily_all, item)
            end
            row = stmt:step()
        end
        stmt:close()
    end)

    local total_days = #daily_all
    local start90 = math.max(1, total_days - 89)
    local start180 = math.max(1, total_days - 179)
    local start365 = math.max(1, total_days - 364)

    for i = start365, total_days do
        local d = daily_all[i]
        if d then
            payload.series.daily_365d[#payload.series.daily_365d + 1] = d
        end
    end

    for i = start180, total_days do
        local d = daily_all[i]
        if d then
            payload.series.daily_180d[#payload.series.daily_180d + 1] = d
        end
    end

    for i = start90, total_days do
        local d = daily_all[i]
        if d then
            table.insert(daily_90, d)
            payload.series.daily_90d[#payload.series.daily_90d + 1] = d
            if (d.duration_sec or 0) > payload.calendar.legend.max_daily_sec_90d then
                payload.calendar.legend.max_daily_sec_90d = d.duration_sec or 0
            end
        end
    end

    for _, d in ipairs(daily_90) do
        if (d.duration_sec or 0) > 0 then
            summary.active_days_90d = summary.active_days_90d + 1
            payload.kpis.longest_day_sec = math.max(payload.kpis.longest_day_sec or 0, d.duration_sec or 0)
        end
    end
    if total_days > 0 then
        summary.last_read_date = daily_all[total_days].date or ""
    end

    for i = math.max(1, total_days - 6), total_days do
        local d = daily_all[i]
        if d then payload.kpis.last_7_days_time_sec = payload.kpis.last_7_days_time_sec + (d.duration_sec or 0) end
    end
    local days30_count = 0
    for i = math.max(1, total_days - 29), total_days do
        local d = daily_all[i]
        if d then
            payload.kpis.last_30_days_time_sec = payload.kpis.last_30_days_time_sec + (d.duration_sec or 0)
            days30_count = days30_count + 1
        end
    end
    if days30_count > 0 then
        payload.kpis.avg_daily_time_30d_sec = math.floor(payload.kpis.last_30_days_time_sec / days30_count)
    end

    pcall(function()
        local stmt = conn:prepare([[
            SELECT day, id_book, total_duration, pages_events FROM (
              SELECT date(start_time, 'unixepoch', 'localtime') as day,
                     id_book,
                     SUM(duration) as total_duration,
                     COUNT(*) as pages_events
              FROM page_stat_data
              WHERE start_time > 0
                AND start_time >= strftime('%s', 'now', 'localtime', '-364 days', 'start of day')
              GROUP BY day, id_book
              ORDER BY day DESC, total_duration DESC
            )
        ]])
        local row = stmt:step()
        while row do
            local day = row[1] and tostring(row[1]) or ""
            local book_id = tonumber(row[2]) or 0
            local duration = tonumber(row[3]) or 0
            local pages_events = tonumber(row[4]) or 0
            if day ~= "" then
                local slot = daily_map_180[day]
                if not slot then
                    slot = { date = day, duration_sec = 0, books_count = 0, top_books = {}, _seen = {} }
                    daily_map_180[day] = slot
                end
                slot._seen[book_id] = true
                if #slot.top_books < 12 then
                    local sb = stats_book_map[book_id] or {}
                    table.insert(slot.top_books, {
                        stats_book_id = book_id,
                        title = sb.title or "",
                        authors = sb.authors or "",
                        md5 = sb.md5 or "",
                        duration_sec = duration,
                    })
                end
                local t = parse_date_ymd(day)
                if t then
                    local age = now_ts - t
                    if age <= (365 * 86400 + 86400) then
                        day_book_map_365[book_id] = true
                        accumulate_top_rollup(top_rollup_365, book_id, duration, pages_events)
                    end
                    if age <= (180 * 86400 + 86400) then
                        day_book_map_180[book_id] = true
                        accumulate_top_rollup(top_rollup_180, book_id, duration, pages_events)
                    end
                    if age <= (90 * 86400 + 86400) then
                        day_book_map_90[book_id] = true
                        accumulate_top_rollup(top_rollup_90, book_id, duration, pages_events)
                    end
                    if age <= (30 * 86400 + 86400) then
                        day_book_map_30[book_id] = true
                        accumulate_top_rollup(top_rollup_30, book_id, duration, pages_events)
                    end
                end
            end
            row = stmt:step()
        end
        stmt:close()
    end)

    for i = start180, total_days do
        local d = daily_all[i]
        if d and d.date then
            local slot = daily_map_180[d.date] or { date = d.date, top_books = {} }
            slot.duration_sec = d.duration_sec or 0
            slot.books_count = d.books_count or 0
            table.insert(payload.calendar.days, {
                date = d.date,
                duration_sec = slot.duration_sec,
                books_count = slot.books_count,
                top_books = slot.top_books or {},
            })
        end
    end

    do
        local wmap = {
            ["1"] = { weekday = "Mon", total = 0, days = 0 },
            ["2"] = { weekday = "Tue", total = 0, days = 0 },
            ["3"] = { weekday = "Wed", total = 0, days = 0 },
            ["4"] = { weekday = "Thu", total = 0, days = 0 },
            ["5"] = { weekday = "Fri", total = 0, days = 0 },
            ["6"] = { weekday = "Sat", total = 0, days = 0 },
            ["0"] = { weekday = "Sun", total = 0, days = 0 },
        }
        for _, d in ipairs(daily_all) do
            local ts = parse_date_ymd(d.date)
            if ts then
                local key = os.date("%w", ts)
                local slot = wmap[key]
                if slot then
                    slot.total = slot.total + (tonumber(d.duration_sec) or 0)
                    slot.days = slot.days + 1
                end
            end
        end
        local order = {"1","2","3","4","5","6","0"}
        for _, k in ipairs(order) do
            local v = wmap[k]
            table.insert(payload.series.weekday_avg, {
                weekday = v.weekday,
                duration_sec = v.days > 0 and math.floor(v.total / v.days) or 0,
            })
        end
    end

    do
        local h30 = build_hourly_slots()
        local h90 = build_hourly_slots()
        local h180 = build_hourly_slots()
        local h365 = build_hourly_slots()

        pcall(function()
            local stmt = conn:prepare([[
                SELECT date(start_time, 'unixepoch', 'localtime') as day,
                       strftime('%H', start_time, 'unixepoch', 'localtime') as hh,
                       COUNT(*) as sessions,
                       SUM(duration) as total_duration
                FROM page_stat_data
                WHERE start_time > 0
                  AND start_time >= strftime('%s', 'now', 'localtime', '-364 days', 'start of day')
                GROUP BY day, hh
                ORDER BY day ASC, hh ASC
            ]])
            local row = stmt:step()
            while row do
                local day = row[1] and tostring(row[1]) or ""
                local hour = tonumber(row[2])
                local sessions = tonumber(row[3]) or 0
                local duration_sec = tonumber(row[4]) or 0
                if day ~= "" and hour and hour >= 0 and hour <= 23 then
                    local ts = parse_date_ymd(day)
                    if ts then
                        local age = now_ts - ts
                        if age <= (365 * 86400 + 86400) then
                            h365[hour].sessions = h365[hour].sessions + sessions
                            h365[hour].duration_sec = h365[hour].duration_sec + duration_sec
                        end
                        if age <= (180 * 86400 + 86400) then
                            h180[hour].sessions = h180[hour].sessions + sessions
                            h180[hour].duration_sec = h180[hour].duration_sec + duration_sec
                        end
                        if age <= (90 * 86400 + 86400) then
                            h90[hour].sessions = h90[hour].sessions + sessions
                            h90[hour].duration_sec = h90[hour].duration_sec + duration_sec
                        end
                        if age <= (30 * 86400 + 86400) then
                            h30[hour].sessions = h30[hour].sessions + sessions
                            h30[hour].duration_sec = h30[hour].duration_sec + duration_sec
                        end
                    end
                end
                row = stmt:step()
            end
            stmt:close()
        end)

        append_hourly_series(payload.series.hourly_activity_30d, h30)
        append_hourly_series(payload.series.hourly_activity_90d, h90)
        append_hourly_series(payload.series.hourly_activity_180d, h180)
        append_hourly_series(payload.series.hourly_activity_365d, h365)
        append_hourly_series(payload.series.hourly_activity, h365)
    end

    do
        local monthly_map = {}
        local monthly_keys = {}
        for _, d in ipairs(daily_all) do
            local ym = tostring(d.date or ""):sub(1, 7)
            if ym ~= "" then
                local slot = monthly_map[ym]
                if not slot then
                    slot = { month = ym, duration_sec = 0, days_read = 0 }
                    monthly_map[ym] = slot
                    table.insert(monthly_keys, ym)
                end
                slot.duration_sec = slot.duration_sec + (tonumber(d.duration_sec) or 0)
                if (tonumber(d.duration_sec) or 0) > 0 then
                    slot.days_read = slot.days_read + 1
                end
            end
        end
        table.sort(monthly_keys)
        local start_idx = math.max(1, #monthly_keys - 11)
        for i = start_idx, #monthly_keys do
            local key = monthly_keys[i]
            if key and monthly_map[key] then
                table.insert(payload.series.monthly_12m, monthly_map[key])
            end
        end
    end

    summary.best_streak_days = 0
    summary.current_streak_days = 0
    do
        local prev_ts = nil
        local current_run = 0
        for i = total_days, 1, -1 do
            local d = daily_all[i]
            if d and (d.duration_sec or 0) > 0 then
                local ts = parse_date_ymd(d.date)
                if ts then
                    if not prev_ts then
                        current_run = 1
                    else
                        local diff = day_diff(prev_ts, ts)
                        if diff == 1 then
                            current_run = current_run + 1
                        else
                            current_run = 1
                        end
                    end
                    if current_run > summary.best_streak_days then
                        summary.best_streak_days = current_run
                    end
                    prev_ts = ts
                end
            end
        end
        local today_noon = parse_date_ymd(os.date("%Y-%m-%d"))
        local streak = 0
        local expected_ts = today_noon
        for i = total_days, 1, -1 do
            local d = daily_all[i]
            if d and (d.duration_sec or 0) > 0 then
                local ts = parse_date_ymd(d.date)
                if ts then
                    if expected_ts and ts == expected_ts then
                        streak = streak + 1
                        expected_ts = expected_ts - 86400
                    elseif expected_ts and ts == expected_ts - 86400 and streak == 0 then
                        -- No reading today yet, but reading yesterday starts current streak.
                        streak = 1
                        expected_ts = ts - 86400
                    elseif streak > 0 then
                        break
                    end
                end
            end
        end
        summary.current_streak_days = streak
    end

    local touched30 = 0
    for _ in pairs(day_book_map_30) do touched30 = touched30 + 1 end
    payload.kpis.books_touched_30d = touched30
    local touched90 = 0
    for _ in pairs(day_book_map_90) do touched90 = touched90 + 1 end
    payload.kpis.books_touched_90d = touched90
    local touched180 = 0
    for _ in pairs(day_book_map_180) do touched180 = touched180 + 1 end
    payload.kpis.books_touched_180d = touched180
    local touched365 = 0
    for _ in pairs(day_book_map_365) do touched365 = touched365 + 1 end
    payload.kpis.books_touched_365d = touched365

    local copy_time = {}
    for i = 1, #stats_books do copy_time[i] = stats_books[i] end
    table.sort(copy_time, sort_desc_num("total_read_time_sec"))
    for i = 1, math.min(8, #copy_time) do
        table.insert(payload.top_books.by_time, copy_time[i])
    end
    local copy_pages = {}
    for i = 1, #stats_books do copy_pages[i] = stats_books[i] end
    table.sort(copy_pages, sort_desc_num("total_read_pages"))
    for i = 1, math.min(8, #copy_pages) do
        table.insert(payload.top_books.by_pages, copy_pages[i])
    end

    local function fill_top_books_range(rollup, out_time, out_pages)
        local list = {}
        for _, item in pairs(rollup or {}) do
            table.insert(list, item)
        end
        local copy_time = {}
        for i = 1, #list do copy_time[i] = list[i] end
        table.sort(copy_time, sort_desc_num("total_read_time_sec"))
        for i = 1, math.min(8, #copy_time) do
            table.insert(out_time, copy_time[i])
        end

        local copy_pages = {}
        for i = 1, #list do copy_pages[i] = list[i] end
        table.sort(copy_pages, function(a, b)
            local ap = tonumber(a.total_read_pages) or 0
            local bp = tonumber(b.total_read_pages) or 0
            if ap == bp then
                return (tonumber(a.total_read_time_sec) or 0) > (tonumber(b.total_read_time_sec) or 0)
            end
            return ap > bp
        end)
        for i = 1, math.min(8, #copy_pages) do
            table.insert(out_pages, copy_pages[i])
        end
    end

    fill_top_books_range(top_rollup_30, payload.top_books.by_time_30d, payload.top_books.by_pages_30d)
    fill_top_books_range(top_rollup_90, payload.top_books.by_time_90d, payload.top_books.by_pages_90d)
    fill_top_books_range(top_rollup_180, payload.top_books.by_time_180d, payload.top_books.by_pages_180d)
    fill_top_books_range(top_rollup_365, payload.top_books.by_time_365d, payload.top_books.by_pages_365d)

    conn:close()
    return cache_dashboard_payload(payload)
end

function DataLoader:loadSidecar(doc_path)
    if not doc_path then return nil end
    local sidecar_dirs = self:getSidecarCandidates(resolve_doc_path(doc_path))

    for _, sdr_dir in ipairs(sidecar_dirs) do
        if lfs.attributes(sdr_dir, "mode") == "directory" then
            for entry in lfs.dir(sdr_dir) do
                if entry:match("^metadata%..*%.lua$") and not entry:match("%.old$") then
                    local fpath = sdr_dir .. "/" .. entry
                    local ok, data = pcall(dofile, fpath)
                    if ok and type(data) == "table" then
                        return data
                    else
                        logger.warn("KoDashboard: Failed to parse", fpath)
                    end
                end
            end
        end
    end
    return nil
end

function DataLoader:getSidecarCandidates(doc_path)
    local candidates = {}
    local base = doc_path:match("(.+)%.[^.]+$") or doc_path
    local filename = doc_path:match("([^/]+)$") or doc_path
    local filename_base = filename:match("(.+)%.[^.]+$") or filename

    table.insert(candidates, base .. ".sdr")

    local data_dir = DataStorage:getDataDir()
    local dir_sdr = data_dir .. "/docsettings" .. base .. ".sdr"
    table.insert(candidates, dir_sdr)

    table.insert(candidates, data_dir .. "/" .. filename_base .. ".sdr")
    table.insert(candidates, data_dir .. "/" .. filename .. ".sdr")

    -- Scan the data directory for any sdr that contains the book's title
    pcall(function()
        local short_name = filename_base
        -- Strip common suffixes like " (Author) (Z-Library)"
        local core = short_name:match("^(.-)%s*%(") or short_name
        if core and #core > 4 then
            for entry in lfs.dir(data_dir) do
                if entry:match("%.sdr$") and entry ~= "." and entry ~= ".." then
                    if entry:find(core, 1, true) then
                        local p = data_dir .. "/" .. entry
                        local dominated = false
                        for _, c in ipairs(candidates) do
                            if c == p then dominated = true; break end
                        end
                        if not dominated then
                            table.insert(candidates, p)
                        end
                    end
                end
            end
        end
    end)

    return candidates
end

function DataLoader:resolveDocPath(doc_path)
    return resolve_doc_path(doc_path)
end

return DataLoader

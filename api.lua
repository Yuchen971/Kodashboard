local DataLoader = require("dataloader")
local logger = require("logger")
local lfs = require("libs/libkoreader-lfs")
local DataStorage = require("datastorage")

local JSON = require("json")
local socket_url = require("socket.url")
local ltn12 = require("ltn12")
local socket = require("socket")
local http = require("socket.http")
local has_socketutil, socketutil = pcall(require, "socketutil")

local CTYPE = {
    JSON = "application/json",
    TEXT = "text/plain",
    JPEG = "image/jpeg",
    PNG = "image/png",
    WEBP = "image/webp",
    GIF = "image/gif",
}

local Api = {}

local OPEN_LIBRARY_SEARCH = "https://openlibrary.org/search.json"
local OPEN_LIBRARY_COVERS = "https://covers.openlibrary.org"

local function trim(s)
    s = tostring(s or "")
    s = s:gsub("^%s+", "")
    s = s:gsub("%s+$", "")
    return s
end

local function slugify_cover_key(s)
    s = trim(s):lower()
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

local normalize_search_query

normalize_search_query = function(s)
    s = trim(s)
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
    return trim(s)
end

local function get_cover_cache_key(book)
    if book and book.md5 and tostring(book.md5) ~= "" then
        return tostring(book.md5), "md5"
    end
    local stem = (book and book.file and book.file:match("([^/]+)$")) or ""
    stem = stem:gsub("%.[^.]+$", "")
    if stem ~= "" then
        return stable_cover_hash_key(stem), "stemhash"
    end
    local t = slugify_cover_key(normalize_search_query(book and book.title or ""))
    local a = slugify_cover_key(normalize_search_query(book and book.authors or ""))
    if t ~= "" and a ~= "" then
        return t .. "--" .. a, "slug"
    end
    if t ~= "" then
        return t, "slug"
    end
    stem = (book and book.file and book.file:match("([^/]+)$")) or ""
    stem = stem:gsub("%.[^.]+$", "")
    stem = slugify_cover_key(stem)
    if stem ~= "" then
        return stem, "filename"
    end
    return nil, "none"
end

local function ensure_dir(path)
    if not path or path == "" then return false end
    if lfs.attributes(path, "mode") == "directory" then return true end
    local parent = path:match("^(.*)/[^/]+$")
    if parent and parent ~= "" and lfs.attributes(parent, "mode") ~= "directory" then
        ensure_dir(parent)
    end
    local ok = lfs.mkdir(path)
    return ok or lfs.attributes(path, "mode") == "directory"
end

local function image_ext_from_ctype(ctype)
    local ct = tostring(ctype or ""):lower()
    if ct:find("webp", 1, true) then return ".webp" end
    if ct:find("png", 1, true) then return ".png" end
    return ".jpg"
end

local function normalize_image_ctype(ctype)
    local ct = tostring(ctype or ""):lower()
    if ct:find("webp", 1, true) then return CTYPE.WEBP end
    if ct:find("png", 1, true) then return CTYPE.PNG end
    if ct:find("jpeg", 1, true) or ct:find("jpg", 1, true) then return CTYPE.JPEG end
    return nil
end

local function image_ctype_from_ext(ext)
    ext = tostring(ext or ""):lower()
    if ext == ".webp" then return CTYPE.WEBP end
    if ext == ".png" then return CTYPE.PNG end
    return CTYPE.JPEG
end

local function detect_image_ctype_from_body(body)
    body = tostring(body or "")
    if #body < 4 then return nil end
    if #body >= 12 and body:sub(1, 4) == "RIFF" and body:sub(9, 12) == "WEBP" then
        return CTYPE.WEBP
    end
    if #body >= 8 and body:sub(1, 8) == "\137PNG\r\n\026\n" then
        return CTYPE.PNG
    end
    if body:sub(1, 2) == "\255\216" then
        return CTYPE.JPEG
    end
    return nil
end

local function http_get(url)
    local chunks = {}
    if has_socketutil then
        socketutil:set_timeout(10, 25)
    end
    local code, headers, status = socket.skip(1, http.request({
        url = url,
        method = "GET",
        sink = has_socketutil and socketutil.table_sink(chunks) or ltn12.sink.table(chunks),
        headers = {
            ["accept"] = "application/json,image/jpeg,image/webp,image/png,*/*",
        },
    }))
    if has_socketutil then
        socketutil:reset_timeout()
    end
    local body = table.concat(chunks)
    local status_code = tonumber(code) or 0
    if status_code < 200 or status_code >= 300 then
        local err = status or ("HTTP " .. tostring(status_code))
        if headers == nil and code then err = tostring(code) end
        return nil, err, headers, status_code
    end
    return body, nil, headers, status_code
end

local function json_get(url)
    local body, err = http_get(url)
    if not body then return nil, err end
    local ok, parsed = pcall(JSON.decode, body)
    if not ok then return nil, "json decode failed" end
    return parsed
end

local function get_cover_storage_dir()
    return (DataStorage:getDataDir() or ".") .. "/kodashboard/covers"
end

local function find_book_in_list_by_id(books, id)
    id = tostring(id or "")
    if type(books) ~= "table" or id == "" then return nil end
    for _, b in ipairs(books) do
        if b and tostring(b.id or "") == id then
            return b
        end
    end
    return nil
end

local function extract_local_cover_to_cache(book)
    if not book or not book.file then
        return nil, "Missing book file"
    end

    local resolved_file = book.file
    pcall(function()
        if DataLoader.resolveDocPath then
            resolved_file = DataLoader:resolveDocPath(book.file) or book.file
        end
    end)
    local file_exists = lfs.attributes(resolved_file, "mode") == "file"
    if not file_exists then
        return nil, "Book file not found: " .. tostring(resolved_file)
    end

    local ok_reg, DocumentRegistry = pcall(require, "document/documentregistry")
    if not ok_reg or not DocumentRegistry then
        return nil, "DocumentRegistry unavailable"
    end

    local doc = nil
    local ok_doc, doc_or_err = pcall(function()
        local d = DocumentRegistry:openDocument(resolved_file)
        if d and d.loadDocument then
            d:loadDocument(false) -- metadata-only load when supported
        end
        return d
    end)
    if ok_doc then doc = doc_or_err end
    if not doc then
        return nil, "Unable to open document"
    end
    local doc_type = tostring(doc.provider or doc.document_type or doc._name or "unknown")

    local cover_bb = nil
    local cover_err = nil

    local function try_get_cover()
        local ok_cover, cover_or_err = pcall(function()
            return doc:getCoverPageImage()
        end)
        if ok_cover and cover_or_err then
            cover_bb = cover_or_err
            return true
        end
        if not ok_cover then
            cover_err = tostring(cover_or_err)
        end
        return false
    end

    -- First attempt (metadata-only for CreDocument)
    try_get_cover()

    -- Some EPUBs expose cover only after a fuller document load.
    if not cover_bb and doc.loadDocument then
        pcall(function() doc:loadDocument() end)
        try_get_cover()
    end

    -- Final fallback: use first page image as cover-ish thumbnail.
    if not cover_bb and doc.getPageImage then
        local ok_page, page_or_err = pcall(function()
            return doc:getPageImage(0)
        end)
        if ok_page and page_or_err then
            cover_bb = page_or_err
        elseif not ok_page then
            cover_err = cover_err or tostring(page_or_err)
        end
    end

    pcall(function() doc:close() end)

    if not cover_bb then
        return nil, "No embedded cover (provider=" .. doc_type .. ")" .. (cover_err and (": " .. cover_err) or "")
    end

    local cover_dir = get_cover_storage_dir()
    if not ensure_dir(cover_dir) then
        return nil, "Unable to create cover dir"
    end

    local cover_key = get_cover_cache_key(book)
    if not cover_key then
        return nil, "No cache key (missing md5/title/file)"
    end
    local jpg_path = string.format("%s/%s.jpg", cover_dir, tostring(cover_key))
    local png_path = string.format("%s/%s.png", cover_dir, tostring(cover_key))

    local saved = false
    local out_path = nil
    local out_ctype = nil

    local ok_write_jpg = pcall(function()
        saved = cover_bb:writeToFile(jpg_path, "jpg", 80, false) and true or false
    end)
    if ok_write_jpg and saved then
        out_path = jpg_path
        out_ctype = CTYPE.JPEG
    else
        local ok_write_png = pcall(function()
            saved = cover_bb:writeToFile(png_path, "png", 90, false) and true or false
        end)
        if ok_write_png and saved then
            out_path = png_path
            out_ctype = CTYPE.PNG
        end
    end

    pcall(function()
        if cover_bb.free then cover_bb:free() end
    end)

    if not saved or not out_path then
        return nil, "Failed to write extracted cover"
    end

    return {
        path = out_path,
        content_type = out_ctype or CTYPE.JPEG,
        source = "embedded",
    }
end

local function download_openlibrary_cover(cover_id)
    local attempts = {
        string.format("%s/b/id/%s-L.webp?default=false", OPEN_LIBRARY_COVERS, cover_id),
        string.format("%s/b/id/%s-L.jpg?default=false", OPEN_LIBRARY_COVERS, cover_id),
    }
    local last_err = nil
    for _, url in ipairs(attempts) do
        local body, err, headers = http_get(url)
        if body then
            local ctype = headers and (headers["content-type"] or headers["Content-Type"]) or nil
            return body, ctype, headers, url
        end
        last_err = err
    end
    return nil, nil, nil, nil, last_err
end

local function first_cover_id_for_query(query)
    local params = string.format(
        "q=%s&limit=5&lang=eng&fields=key,cover_i",
        socket_url.escape(query or "")
    )
    local data, err = json_get(OPEN_LIBRARY_SEARCH .. "?" .. params)
    if not data or type(data.docs) ~= "table" then
        return nil, err or "no results"
    end
    for _, doc in ipairs(data.docs) do
        if doc.cover_i then
            return tostring(doc.cover_i)
        end
    end
    return nil, "no cover id"
end

local function first_cover_id_for_book(book)
    local raw_title = trim(book.title or "")
    local raw_authors = trim(book.authors or "")
    local clean_title = normalize_search_query(raw_title)
    local clean_authors = normalize_search_query(raw_authors)

    local tried = {}
    local function try_query(q)
        q = trim(q)
        if q == "" or tried[q] then return nil end
        tried[q] = true
        local cover_id, err = first_cover_id_for_query(q)
        if cover_id then
            return cover_id, nil, q
        end
        return nil, err, q
    end

    local candidates = {
        trim(raw_title .. " " .. raw_authors),
        trim(clean_title .. " " .. clean_authors),
        clean_title,
        raw_title,
    }

    -- Extra fallback for titles with separators, e.g. "A | B", "A - B"
    if clean_title:find("|", 1, true) then
        table.insert(candidates, trim((clean_title:gsub("|.*$", ""))))
    end
    local split_dash = trim((clean_title:gsub("%s+%-.*$", "")))
    if split_dash ~= clean_title then
        table.insert(candidates, split_dash)
    end

    local last_err, last_query = nil, nil
    for _, q in ipairs(candidates) do
        local cover_id, err, used_q = try_query(q)
        if cover_id then
            return cover_id, used_q
        end
        last_err, last_query = err, used_q
    end

    return nil, (last_err or "no cover id"), last_query
end

local function delete_existing_cover_variants(cover_dir, cover_key, keep_ext)
    local exts = { ".jpg", ".jpeg", ".png", ".webp", ".gif" }
    for _, ext in ipairs(exts) do
        if ext ~= keep_ext then
            pcall(function()
                os.remove(string.format("%s/%s%s", cover_dir, tostring(cover_key), ext))
            end)
        end
    end
end

local function save_uploaded_cover(book_ref, body, header_ctype)
    local books = DataLoader:getBooks()
    local book = find_book_in_list_by_id(books, book_ref)
    if not book then
        return { ok = false, error = "Book not found" }, 404
    end

    body = tostring(body or "")
    local body_size = #body
    if body_size <= 0 then
        return { ok = false, error = "Empty body" }, 400
    end
    if body_size > (2 * 1024 * 1024) then
        return { ok = false, error = "Cover payload too large (>2MB)" }, 413
    end

    local sniffed_ctype = detect_image_ctype_from_body(body)
    local declared_ctype = normalize_image_ctype(header_ctype)
    local ctype = sniffed_ctype or declared_ctype
    if ctype ~= CTYPE.JPEG and ctype ~= CTYPE.PNG and ctype ~= CTYPE.WEBP then
        return { ok = false, error = "Unsupported image format" }, 415
    end

    local cover_dir = get_cover_storage_dir()
    if not ensure_dir(cover_dir) then
        return { ok = false, error = "Unable to create cover dir" }, 500
    end

    local cover_key = get_cover_cache_key(book)
    if not cover_key then
        return { ok = false, error = "No cache key for cover" }, 500
    end

    local ext = image_ext_from_ctype(ctype)
    local path = string.format("%s/%s%s", cover_dir, tostring(cover_key), ext)
    local f = io.open(path, "wb")
    if not f then
        return { ok = false, error = "Unable to save cover", path = path }, 500
    end
    f:write(body)
    f:close()

    delete_existing_cover_variants(cover_dir, cover_key, ext)

    return {
        ok = true,
        saved = true,
        md5 = book.md5,
        cover_cache_key = cover_key,
        path = path,
        content_type = ctype,
        source = "upload",
    }, 200
end

function Api.handleRequest(server, reqinfo, path, full_uri)
    local cover_book_ref = path:match("^/api/books/([^/]+)/cover$")
    if cover_book_ref then
        return Api.sendBookCover(server, reqinfo, cover_book_ref)
    end

    local upload_cover_book_ref = path:match("^/api/books/([^/]+)/upload%-cover$")
    if upload_cover_book_ref then
        if reqinfo.method ~= "POST" then
            return server:sendResponse(reqinfo, 405, CTYPE.JSON, '{"ok":false,"error":"Only POST supported"}')
        end
        local payload, status = save_uploaded_cover(
            upload_cover_book_ref,
            reqinfo.body or "",
            reqinfo.headers and reqinfo.headers["content-type"] or nil
        )
        local enc_ok, json_str = pcall(JSON.encode, payload or { ok = false, error = "encode failure" })
        if not enc_ok then
            return server:sendResponse(reqinfo, 500, CTYPE.JSON, '{"ok":false,"error":"json encoding failed"}')
        end
        return server:sendResponse(reqinfo, status or 200, CTYPE.JSON, json_str)
    end

    local ok, result = xpcall(function()
        return Api.route(path, full_uri, reqinfo)
    end, function(err)
        if debug and debug.traceback then
            return debug.traceback(tostring(err), 2)
        end
        return tostring(err)
    end)

    if not ok then
        logger.err("KoDashboard API error:", result)
        local enc_ok, err_body = pcall(JSON.encode, {
            error = "internal server error",
            detail = tostring(result),
        })
        if enc_ok then
            return server:sendResponse(reqinfo, 500, CTYPE.JSON, err_body)
        end
        return server:sendResponse(reqinfo, 500, CTYPE.JSON,
            '{"error":"internal server error","detail":"failed to encode error"}')
    end

    local enc_ok, json_str = pcall(JSON.encode, result)
    if not enc_ok then
        logger.err("KoDashboard JSON encode error:", json_str)
        return server:sendResponse(reqinfo, 500, CTYPE.JSON,
            '{"error":"json encoding failed: ' .. tostring(json_str):gsub('"', '\\"'):gsub('\n', ' ') .. '"}')
    end

    return server:sendResponse(reqinfo, 200, CTYPE.JSON, json_str)
end

function Api.sendBookCover(server, reqinfo, book_ref)
    local ok, cover_info = pcall(function()
        return DataLoader:getBookCover(book_ref)
    end)
    if not ok then
        logger.err("KoDashboard cover API error:", cover_info)
        return server:sendResponse(reqinfo, 500, CTYPE.TEXT, "cover lookup failed")
    end
    if not cover_info or not cover_info.path then
        return server:sendResponse(reqinfo, 404, CTYPE.TEXT, "cover not found")
    end

    local f = io.open(cover_info.path, "rb")
    if not f then
        return server:sendResponse(reqinfo, 404, CTYPE.TEXT, "cover not readable")
    end
    local body = f:read("*all")
    f:close()
    return server:sendResponse(reqinfo, 200, cover_info.content_type or CTYPE.JPEG, body)
end

function Api.route(path, full_uri, reqinfo)
    if reqinfo and reqinfo.method ~= "GET" then
        return { error = "Method not allowed: " .. tostring(reqinfo.method) }
    end
    if path == "/api/books" then
        return Api.getBooks()
    end

    local book_ref = path:match("^/api/books/([^/]+)$")
    if book_ref then
        return Api.getBook(book_ref)
    end

    local ann_book_ref = path:match("^/api/books/([^/]+)/annotations$")
    if ann_book_ref then
        return Api.getBookAnnotations(ann_book_ref)
    end

    local fetch_cover_book_ref = path:match("^/api/books/([^/]+)/fetch%-cover$")
    if fetch_cover_book_ref then
        return Api.fetchBookCover(fetch_cover_book_ref)
    end

    local timeline_book_ref = path:match("^/api/books/([^/]+)/timeline$")
    if timeline_book_ref then
        return Api.getBookTimeline(timeline_book_ref)
    end

    if path == "/api/highlights" then
        return Api.getAllHighlights()
    end

    if path == "/api/stats" then
        return Api.getStats()
    end

    if path == "/api/overview" then
        return Api.getOverview()
    end

    if path == "/api/dashboard" then
        return Api.getDashboard()
    end

    return { error = "Unknown API endpoint: " .. path }
end

function Api.getBooks()
    local books = DataLoader:getBooks()
    return { books = books, total = #books }
end

function Api.getBook(book_ref)
    local books = DataLoader:getBooks()
    local book = find_book_in_list_by_id(books, book_ref)
    if not book then return { error = "Book not found" } end
    return book
end

function Api.getBookAnnotations(book_ref)
    local annotations, doc_props = DataLoader:getAnnotations(book_ref)
    if not annotations then
        return { error = "Book not found" }
    end
    local title = ""
    local authors = ""
    if doc_props then
        title = doc_props.title or ""
        authors = doc_props.authors or ""
    end
    return {
        book_id = tostring(book_ref or ""),
        book_ref = tostring(book_ref or ""),
        title = title,
        authors = authors,
        annotations = annotations,
        total = #annotations,
    }
end

function Api.getBookTimeline(book_ref)
    local timeline = DataLoader:getBookTimeline(book_ref)
    if not timeline then
        return { error = "Book not found" }
    end
    return timeline
end

function Api.fetchBookCover(book_ref)
    local books = DataLoader:getBooks()
    local book = find_book_in_list_by_id(books, book_ref)
    if not book then
        return { ok = false, error = "Book not found" }
    end

    local existing = DataLoader:getBookCover(book_ref)
    if existing and existing.path then
        return {
            ok = true,
            skipped = true,
            reason = "cover_exists",
            md5 = book.md5,
            path = existing.path,
            content_type = existing.content_type,
            source = "existing",
        }
    end

    -- Local-first: try extracting embedded cover before network.
    local embedded_first, emb_first_err = extract_local_cover_to_cache(book)
    if embedded_first then
        return {
            ok = true,
            saved = true,
            md5 = book.md5,
            source = "embedded",
            path = embedded_first.path,
            content_type = embedded_first.content_type,
        }
    end

    local query = trim(string.format("%s %s", book.title or "", book.authors or ""))
    if query == "" then
        return { ok = false, error = "Missing title/authors for search", fallback_error = emb_first_err, md5 = book.md5 }
    end

    local cover_id, search_meta, used_query = first_cover_id_for_book(book)
    if not cover_id then
        local embedded, emb_err = extract_local_cover_to_cache(book)
        if embedded then
            return {
                ok = true,
                saved = true,
                md5 = book.md5,
                query = query,
                tried_query = used_query,
                source = "embedded",
                path = embedded.path,
                content_type = embedded.content_type,
            }
        end
        return {
            ok = false,
            error = (search_meta or "No cover found") .. (emb_first_err and (" (local fallback: " .. emb_first_err .. ")") or ""),
            fallback_error = emb_first_err,
            md5 = book.md5,
            query = query,
            tried_query = used_query,
        }
    end

    local cover_body, ctype, _, fetched_url, cover_err = download_openlibrary_cover(cover_id)
    if not cover_body then
        local embedded, emb_err = extract_local_cover_to_cache(book)
        if embedded then
            return {
                ok = true,
                saved = true,
                md5 = book.md5,
                cover_id = cover_id,
                source = "embedded",
                path = embedded.path,
                content_type = embedded.content_type,
                cover_error = cover_err,
                fetched_url = fetched_url,
            }
        end
        return {
            ok = false,
            error = (cover_err or "Download failed") .. (emb_err and (" (epub fallback: " .. emb_err .. ")") or ""),
            fallback_error = emb_err,
            md5 = book.md5,
            cover_id = cover_id,
            fetched_url = fetched_url,
        }
    end

    local cover_dir = get_cover_storage_dir()
    if not ensure_dir(cover_dir) then
        return { ok = false, error = "Unable to create cover dir", md5 = book.md5 }
    end

    local ext = image_ext_from_ctype(ctype)

    local cover_key = get_cover_cache_key(book)
    if not cover_key then
        return { ok = false, error = "No cache key for cover", md5 = book.md5 }
    end
    local path = string.format("%s/%s%s", cover_dir, tostring(cover_key), ext)
    local f = io.open(path, "wb")
    if not f then
        return { ok = false, error = "Unable to save cover", md5 = book.md5, path = path }
    end
    f:write(cover_body)
    f:close()

    return {
        ok = true,
        saved = true,
        md5 = book.md5,
        cover_cache_key = cover_key,
        cover_id = cover_id,
        path = path,
        query = query,
        used_query = used_query,
        content_type = image_ctype_from_ext(ext),
        source = "openlibrary",
        fetched_url = fetched_url,
    }
end

function Api.getAllHighlights()
    local highlights = DataLoader:getAllHighlights()
    return { highlights = highlights, total = #highlights }
end

function Api.getStats()
    return DataLoader:getStats()
end

function Api.getOverview()
    local books = DataLoader:getBooks()
    local total_books = #books
    local reading = 0
    local finished = 0
    local total_highlights = 0
    local total_notes = 0

    for _, b in ipairs(books) do
        if b.status == "complete" or b.status == "finished" then
            finished = finished + 1
        elseif b.percent > 0 then
            reading = reading + 1
        end
        total_highlights = total_highlights + (b.highlights or 0)
        total_notes = total_notes + (b.notes or 0)
    end

    local stats_ok, stats = pcall(function() return DataLoader:getStats() end)
    if not stats_ok then
        stats = { books = {}, daily = {} }
    end

    local total_read_time = 0
    if stats.books then
        for _, s in ipairs(stats.books) do
            total_read_time = total_read_time + (s.total_read_time or 0)
        end
    end

    return {
        total_books = total_books,
        reading = reading,
        finished = finished,
        total_highlights = total_highlights,
        total_notes = total_notes,
        total_read_time = total_read_time,
        recent_days = stats.daily or {},
    }
end

function Api.getDashboard()
    return DataLoader:getDashboard()
end

return Api

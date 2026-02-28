local Device = require("device")
local UIManager = require("ui/uimanager")
local WidgetContainer = require("ui/widget/container/widgetcontainer")
local QRMessage = require("ui/widget/qrmessage")
local Event = require("ui/event")
local logger = require("logger")
local util = require("util")
local _ = require("gettext")
local T = require("ffi/util").template

local KoDashboard = WidgetContainer:extend{
    name = "kodashboard",
    is_doc_only = false,
}

local HTTP_RESPONSE_CODE = {
    [200] = "OK",
    [302] = "Found",
    [404] = "Not Found",
    [405] = "Method Not Allowed",
    [500] = "Internal Server Error",
}

local CTYPE = {
    CSS  = "text/css",
    HTML = "text/html",
    JS   = "application/javascript",
    JSON = "application/json",
    PNG  = "image/png",
    TEXT = "text/plain",
}

local EXT_TO_CTYPE = {
    [".html"] = CTYPE.HTML,
    [".css"]  = CTYPE.CSS,
    [".js"]   = CTYPE.JS,
    [".json"] = CTYPE.JSON,
    [".png"]  = CTYPE.PNG,
    [".svg"]  = "image/svg+xml",
}

function KoDashboard:init()
    self.port = G_reader_settings:readSetting("kodashboard_port", "8686")
    self.ui.menu:registerToMainMenu(self)
end

function KoDashboard:isRunning()
    return self.http_socket ~= nil
end

function KoDashboard:onEnterStandby()
    if self:isRunning() then self:stop() end
end

function KoDashboard:onSuspend()
    if self:isRunning() then self:stop() end
end

function KoDashboard:onExit()
    if self:isRunning() then self:stop() end
end

function KoDashboard:onCloseWidget()
    if self:isRunning() then self:stop() end
end

function KoDashboard:start()
    logger.dbg("KoDashboard: Starting server...")

    if Device:isKindle() then
        os.execute(string.format(
            "iptables -A INPUT -p tcp --dport %s -m conntrack --ctstate NEW,ESTABLISHED -j ACCEPT",
            self.port))
        os.execute(string.format(
            "iptables -A OUTPUT -p tcp --sport %s -m conntrack --ctstate ESTABLISHED -j ACCEPT",
            self.port))
    end

    local ServerClass = require("ui/message/simpletcpserver")
    self.http_socket = ServerClass:new{
        host = "*",
        port = self.port,
        receiveCallback = function(data, id) return self:onRequest(data, id) end,
    }
    local ok, err = self.http_socket:start()
    if ok then
        self.http_messagequeue = UIManager:insertZMQ(self.http_socket)
        logger.dbg("KoDashboard: Server listening on port " .. self.port)
    else
        logger.err("KoDashboard: Failed to start server:", err)
        self.http_socket = nil
        local InfoMessage = require("ui/widget/infomessage")
        UIManager:show(InfoMessage:new{
            text = T(_("Failed to start KoDashboard on port %1."), self.port) .. "\n\n" .. err,
        })
    end
end

function KoDashboard:stop()
    logger.dbg("KoDashboard: Stopping server...")

    if Device:isKindle() then
        os.execute(string.format(
            "iptables -D INPUT -p tcp --dport %s -m conntrack --ctstate NEW,ESTABLISHED -j ACCEPT",
            self.port))
        os.execute(string.format(
            "iptables -D OUTPUT -p tcp --sport %s -m conntrack --ctstate ESTABLISHED -j ACCEPT",
            self.port))
    end

    if self.http_socket then
        self.http_socket:stop()
        self.http_socket = nil
    end
    if self.http_messagequeue then
        UIManager:removeZMQ(self.http_messagequeue)
        self.http_messagequeue = nil
    end
    logger.dbg("KoDashboard: Server stopped.")
end

function KoDashboard:showQRCode()
    if not self:isRunning() then
        self:start()
    end
    if not self:isRunning() then
        return
    end
    local ip = self:getIP()
    if not ip then
        local InfoMessage = require("ui/widget/infomessage")
        UIManager:show(InfoMessage:new{
            text = _("No network IP detected. Connect to Wi-Fi and try again."),
        })
        return
    end
    local qr_size = math.floor(math.min(Device.screen:getWidth(), Device.screen:getHeight()) * 0.50)
    UIManager:show(QRMessage:new{
        text = T("http://%1:%2", ip, self.port),
        width = qr_size,
        height = qr_size,
    })
end

function KoDashboard:addToMainMenu(menu_items)
    menu_items.kodashboard = {
        text = _("KoDashboard"),
        sorting_hint = "tools",
        sub_item_table = {
            {
                text_func = function()
                    if self:isRunning() then
                        return _("Stop dashboard server")
                    else
                        return _("Start dashboard server")
                    end
                end,
                keep_menu_open = true,
                callback = function(touchmenu_instance)
                    if self:isRunning() then
                        self:stop()
                    else
                        self:start()
                    end
                    if touchmenu_instance then
                        touchmenu_instance:updateItems()
                    end
                end,
            },
            {
                text_func = function()
                    if self:isRunning() then
                        return _("Show QR code")
                    end
                    return _("Show QR code (starts server)")
                end,
                keep_menu_open = true,
                callback = function(touchmenu_instance)
                    self:showQRCode()
                    if touchmenu_instance then
                        touchmenu_instance:updateItems()
                    end
                end,
            },
            {
                text_func = function()
                    if self:isRunning() then
                        local ip = self:getIP()
                        if ip then
                            return T(_("Open http://%1:%2"), ip, self.port)
                        end
                        return T(_("Listening on port %1"), self.port)
                    else
                        return _("Not running")
                    end
                end,
                enabled_func = function() return false end,
                separator = true,
            },
            {
                text_func = function()
                    return T(_("Port: %1"), self.port)
                end,
                keep_menu_open = true,
                callback = function(touchmenu_instance)
                    local InputDialog = require("ui/widget/inputdialog")
                    local port_dialog
                    port_dialog = InputDialog:new{
                        title = _("Set custom port"),
                        input = self.port,
                        input_type = "number",
                        buttons = {{
                            {
                                text = _("Cancel"),
                                id = "close",
                                callback = function()
                                    UIManager:close(port_dialog)
                                end,
                            },
                            {
                                text = _("Save"),
                                is_enter_default = true,
                                callback = function()
                                    local new_port = port_dialog:getInputText()
                                    UIManager:close(port_dialog)
                                    if new_port and new_port ~= "" then
                                        self.port = new_port
                                        G_reader_settings:saveSetting("kodashboard_port", new_port)
                                    end
                                    if touchmenu_instance then
                                        touchmenu_instance:updateItems()
                                    end
                                end,
                            },
                        }},
                    }
                    UIManager:show(port_dialog)
                end,
            },
        },
    }
end

function KoDashboard:getIP()
    local socket = require("socket")
    local s = socket.udp()
    s:setpeername("10.255.255.255", 1)
    local ip = s:getsockname()
    s:close()
    if ip and ip ~= "0.0.0.0" then
        return ip
    end
    return nil
end

function KoDashboard:sendResponse(reqinfo, http_code, content_type, body)
    if not http_code then http_code = 400 end
    if not body then body = "" end
    if type(body) ~= "string" then body = tostring(body) end

    local response = {}
    table.insert(response, T("HTTP/1.0 %1 %2", http_code, HTTP_RESPONSE_CODE[http_code] or "Unspecified"))
    if content_type then
        local charset = ""
        if util.stringStartsWith(content_type, "text/") or content_type == CTYPE.JSON then
            charset = "; charset=utf-8"
        end
        table.insert(response, T("Content-Type: %1%2", content_type, charset))
    end
    if http_code == 302 then
        table.insert(response, T("Location: %1", body))
        body = ""
    end
    table.insert(response, "Access-Control-Allow-Origin: *")
    table.insert(response, "Cache-Control: no-store, no-cache, must-revalidate, max-age=0")
    table.insert(response, "Pragma: no-cache")
    table.insert(response, "Expires: 0")
    table.insert(response, T("Content-Length: %1", #body))
    table.insert(response, "Connection: close")
    table.insert(response, "")
    table.insert(response, body)
    response = table.concat(response, "\r\n")
    if self.http_socket then
        self.http_socket:send(response, reqinfo.request_id)
    end
    return Event:new("InputEvent")
end

function KoDashboard:onRequest(data, request_id)
    local reqinfo = { request_id = request_id }
    local head, body = data:match("^(.-)\r?\n\r?\n(.*)$")
    head = head or data
    body = body or ""

    local method, uri = head:match("^(%u+)%s+([^%s]+)%s+HTTP/%d%.%d")
    if not method or not uri then
        return self:sendResponse(reqinfo, 400, CTYPE.TEXT, "Malformed request")
    end

    local headers = {}
    for line in head:gmatch("\r?\n([^\r\n]+)") do
        local k, v = line:match("^%s*([^:]+):%s*(.*)$")
        if k and v then
            headers[tostring(k):lower()] = v
        end
    end

    reqinfo.method = method
    reqinfo.headers = headers
    reqinfo.body = body

    if method == "POST" then
        local clen = tonumber(headers["content-length"] or "0") or 0
        if clen < 0 then clen = 0 end
        if #reqinfo.body < clen and request_id and request_id.receive then
            local remain = clen - #reqinfo.body
            local chunks = { reqinfo.body }
            while remain > 0 do
                local part, err, partial = request_id:receive(remain)
                if part and #part > 0 then
                    table.insert(chunks, part)
                    remain = remain - #part
                elseif partial and #partial > 0 then
                    table.insert(chunks, partial)
                    remain = remain - #partial
                else
                    logger.warn("KoDashboard: failed reading POST body:", err or "unknown")
                    break
                end
            end
            reqinfo.body = table.concat(chunks)
        end
    end

    if method ~= "GET" and method ~= "POST" then
        return self:sendResponse(reqinfo, 405, CTYPE.TEXT, "Only GET/POST supported")
    end

    uri = util.urlDecode(uri)
    -- strip query string for routing
    local path = uri:match("^([^?]*)") or uri

    -- API routes
    if util.stringStartsWith(path, "/api/") then
        local ok_api, api = pcall(require, "api")
        if not ok_api or type(api) ~= "table" then
            logger.err("KoDashboard: failed to load api module:", tostring(api))
            return self:sendResponse(reqinfo, 500, CTYPE.JSON, '{"error":"api module load failed"}')
        end

        if type(api.handleRequest) == "function" then
            return api.handleRequest(self, reqinfo, path, uri)
        end

        -- Backward compatibility for mixed plugin files where api.lua is older.
        if type(api.route) == "function" then
            logger.warn("KoDashboard: api.handleRequest missing, falling back to api.route")
            local JSON = require("json")
            local ok_route, payload = xpcall(function()
                return api.route(path, uri, reqinfo)
            end, function(err)
                if debug and debug.traceback then
                    return debug.traceback(tostring(err), 2)
                end
                return tostring(err)
            end)
            if not ok_route then
                logger.err("KoDashboard: legacy api.route error:", payload)
                local enc_ok, err_body = pcall(JSON.encode, {
                    error = "internal server error",
                    detail = tostring(payload),
                })
                if enc_ok then
                    return self:sendResponse(reqinfo, 500, CTYPE.JSON, err_body)
                end
                return self:sendResponse(reqinfo, 500, CTYPE.JSON,
                    '{"error":"internal server error","detail":"failed to encode error"}')
            end

            local enc_ok, json_str = pcall(JSON.encode, payload)
            if not enc_ok then
                logger.err("KoDashboard: legacy api JSON encode error:", json_str)
                return self:sendResponse(reqinfo, 500, CTYPE.JSON, '{"error":"json encoding failed"}')
            end
            return self:sendResponse(reqinfo, 200, CTYPE.JSON, json_str)
        end

        logger.err("KoDashboard: api module missing request handlers")
        return self:sendResponse(reqinfo, 500, CTYPE.JSON, '{"error":"api handler missing"}')
    end

    -- Static file serving from plugin's web/ directory
    if path == "/" then
        path = "/index.html"
    end
    local plugin_dir = self:getPluginDir()
    local filepath = plugin_dir .. "/web" .. path
    if method ~= "GET" then
        return self:sendResponse(reqinfo, 405, CTYPE.TEXT, "Method not allowed")
    end
    local f = io.open(filepath, "rb")
    if f then
        local content = f:read("*all")
        f:close()
        local ext = path:match("(%.[^.]+)$") or ""
        local ctype = EXT_TO_CTYPE[ext]
        return self:sendResponse(reqinfo, 200, ctype, content)
    end

    return self:sendResponse(reqinfo, 404, CTYPE.TEXT, "Not found: " .. path)
end

function KoDashboard:getPluginDir()
    local info = debug.getinfo(1, "S")
    local plugin_path = info.source:match("@(.*/)")
    return plugin_path or "."
end

return KoDashboard

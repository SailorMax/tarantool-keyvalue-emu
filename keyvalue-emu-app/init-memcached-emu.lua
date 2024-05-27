#!/usr/bin/env tarantool

local strict = require('strict')
strict.on()

-- local box = require('box')
local CACHE = require('init-cache')

local os = require('os')
local log = require('log')
local json = require('json')
local socket = require('socket')
local string = require('string')
require('tools')

local Memcached = {}
local append = table.insert  -- speedup by not using global variable
if not table.unpack then
	table.unpack = unpack
end


-- redis requests handler
-- https://www.tarantool.io/en/doc/latest/reference/reference_lua/socket/#use-tcp-server-to-accept-file-contents-sent-with-socat
socket.tcp_server('0.0.0.0', 11211, {
	prepare = function(sock)
		log.info('Memcached port handler(' .. sock:fd() .. ') initialized.')
		-- sock:setsockopt('SOL_SOCKET', 'SO_DEBUG', true)
		-- sock:setsockopt('SOL_SOCKET', 'SO_REUSEADDR', true)  -- to use same port for 0.0.0.0 as some concrete ip at once
		-- return 5  -- queue size
	end,

	handler = function(sock, client)
		-- log.info('+ client: ' .. client.host .. ':' .. client.port .. ' via ' .. client.family);
		local CRLF = "\r\n"
		local MemcachedFork = {
			STORAGE = CACHE,
			output = {},
			user_cfg = {},
			disconnect = false
		}
		setmetatable(MemcachedFork, { __index=Memcached })  -- undefined attr => look at Memcached (prototype-like)

		local request;
		while true do
			request = sock:read(CRLF);
			-- log.info(json.encode(request))
			if request == '' or request == nil then
				local errno = sock:errno()
				if errno > 0 then
					log.info('error ' .. errno .. ': ' .. sock:error())
				end
				break;
			end

			-- handle
			MemcachedFork:Command(sock, request)
			-- answer
			sock:write(table.concat(MemcachedFork:FlushOutput(), CRLF) .. CRLF)

			if MemcachedFork.disconnect then
				break
			end

		end
		-- log.info('- client: ' .. client.host .. ':' .. client.port .. ' via ' .. client.family);
	end
})

-- https://github.com/memcached/memcached/wiki/Commands
function Memcached:Command(sock, request)
	-- Memcached command: <cm> <key> <datalen*> <flag1> <flag2> <...>\r\n
	local cmd = SplitString(' ', request:sub(1, #request-2))

	-- log.info('> command:')
	-- log.info(cmd)
	local cmd_name = string.upper(cmd[1])

	-- GET is most frequently used => check in first place
	if cmd_name == 'GET' then
		self:OutText(self.STORAGE:Get(cmd[2]));
		self:OutText('END');

	elseif cmd_name == 'SET' or cmd_name == 'ADD' or cmd_name == 'REPLACE' then
		-- set keyname [flags] [ttl] [size]
		local key = cmd[2]
		local flags = cmd[3]  -- TODO: use it
		local ttl = tonumber(cmd[4], 10)
		local length = tonumber(cmd[5], 10)
		local value = sock:read(length)
		sock:read(2)  -- CRLF

		local result = false
		if cmd_name == 'ADD' then
			result = self.STORAGE:Add(key, value, ttl)
		elseif cmd_name == 'REPLACE' then
			result = self.STORAGE:Update(key, value, ttl)
		else -- SET
			result = self.STORAGE:Set(key, value, ttl)
		end

		if result then
			self:OutText('STORED')
		else
			self:OutText("CLIENT_ERROR Can't store the value.")
		end

	elseif cmd_name == 'DELETE' then
		local key = cmd[2]
		local prev_tuple = self.STORAGE:Delete(key)
		if cmd[3] ~= 'noreply' then
			if prev_tuple then
				self:OutText('DELETED')
			else
				self:OutText('NOT_FOUND')
			end
		end

	elseif cmd_name == 'VERSION' then
		self:OutText(GetTarantoolVersion());

	elseif cmd_name == 'STATS' then
		local stats = ''
		if cmd[2] == '' then
			stats = 'STAT version ' .. GetTarantoolVersion() .. "\r\n"
					.. 'STAT time ' .. os.time() .. "\r\n"
					.. 'STAT limit_maxbytes ' .. Get_memtx_max_tuple_size() .. "\r\n"
					.. 'END'
		else
			stats = 'STAT ' .. cmd[2] .. " -\r\n"
					.. 'END'
		end
		self:OutText(stats);

	elseif cmd_name == 'QUIT' then
		self.disconnect = true

	else
		self:OutText("CLIENT_ERROR Not supported command: " .. cmd[1])
	end
end

--
-- https://github.com/memcached/memcached/blob/master/doc/protocol.txt

function Memcached:OutAny(arg)
	-- log.info('< return:')
	-- log.info(arg)
	local arg_type = type(arg)
	if arg_type == 'nil' then
		return self:OutNil()
	else
		return self:OutText(arg)
	end
end

function Memcached:Output(chunk)
	append(self.output, chunk)
end

function Memcached:FlushOutput()
	local buff = self.output
	self.output = {}
	return buff
end

function Memcached:OutNil()
	self:Output('END')
end

function Memcached:OutText(arg)
	if arg then
		self:Output(arg)
	else
		self:OutNil()
	end
end

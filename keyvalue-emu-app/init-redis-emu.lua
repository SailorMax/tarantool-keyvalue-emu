#!/usr/bin/env tarantool

local strict = require('strict')
strict.on()

-- local box = require('box')
local CACHE = require('init-cache')
local SESSIONS = require('init-sessions')
local STORAGES = {
	['CACHE'] = CACHE,
	['SESSIONS'] = SESSIONS
}

local log = require('log')
local json = require('json')
local clock = require('clock')
local socket = require('socket')
local string = require('string')
local config = require('config')
require('tools')

local Redis = {}
local append = table.insert  -- speedup by not using global variable
if not table.unpack then
	table.unpack = unpack
end

-- redis requests handler
-- https://www.tarantool.io/en/doc/latest/reference/reference_lua/socket/#use-tcp-server-to-accept-file-contents-sent-with-socat
socket.tcp_server('0.0.0.0', 6379, {
	prepare = function(sock)
		log.info('Redis port handler(' .. sock:fd() .. ') initialized.')
		-- sock:setsockopt('SOL_SOCKET', 'SO_DEBUG', true)
		-- sock:setsockopt('SOL_SOCKET', 'SO_REUSEADDR', true)  -- to use same port for 0.0.0.0 as some concrete ip at once
		-- return 5  -- queue size
	end,

	handler = function(sock, client)
		-- log.info('+ client: ' .. client.host .. ':' .. client.port .. ' via ' .. client.family);
		local CRLF = "\r\n"
		local RedisFork = {
			STORAGE = CACHE,
			output = {},
			user_cfg = {},
			authenticated = false,
			-- require_authentication = true,
			disconnect = false
		}
		setmetatable(RedisFork, { __index=Redis })  -- undefined attr => look at Redis (prototype-like)

		local guest_cfg = config:get('credentials.users.guest')
		if guest_cfg and (not guest_cfg.password or guest_cfg.password == '') then
			RedisFork.require_authentication = false
		end

		local request;
		while true do
			request = sock:read(CRLF);
			-- request = sock:read(512);
			if request == '' or request == nil then
				local errno = sock:errno()
				if errno > 0 then
					log.info('error ' .. errno .. ': ' .. sock:error())
				end
				break;
			end

			-- Redis command: *[num words]\r\n$[word length]\r\n[word]\r\n\r\n$[word length]\r\n[word]\r\n..
			if request:sub(1, 1) == '*' then  -- list
				-- get number of words
				local cmd_cnt = tonumber(request:sub(2), 10)
				local cmd = {}
				cmd[cmd_cnt] = false  -- pre-calc hash for all table
				for i=1, cmd_cnt do  -- Lua's arrays starts from 1
					-- get word length
					request = sock:read(CRLF)
					if request:sub(1, 1) == '$' then
						-- get word by it length
						cmd[i] = sock:read(tonumber(request:sub(2), 10))
						sock:read(2)  -- \r\n
					else
						log.info('unexpected: ' .. request .. '(' .. Bin2hex(request) .. ')')
					end
				end

				-- handle
				RedisFork:Command(cmd)
				-- answer
				sock:write(table.concat(RedisFork:FlushOutput(), CRLF) .. CRLF)

				if RedisFork.disconnect then
					break
				end

			else
				log.info('received unknown: ' .. request)
				log.info(Bin2hex(request))
			end
		end
		-- log.info('- client: ' .. client.host .. ':' .. client.port .. ' via ' .. client.family);
	end
})

-- https://valkey.io/commands/
function Redis:Command(cmd)
	-- log.info('> command:')
	-- log.info(cmd)
	local cmd_name = cmd[1]

	if self.require_authentication and not self.authenticated then
		if cmd_name ~= 'PING' and cmd_name ~= 'AUTH' and cmd_name ~= 'QUIT' then
			self:OutErrorString("Forbidden")
			return
		end
	end

	-- GET and FCALL is most frequently used => check in first place
	if cmd_name == 'GET' then -- https://valkey.io/commands/get/
		self:OutText(self.STORAGE:Get(cmd[2]));

	elseif cmd_name == 'FCALL' then  -- https://valkey.io/commands/fcall/
		-- log.info(cmd)
		local args = {}
		-- just skip keys. Our functions doesn't use its
		local args_start_idx = 3 + tonumber(cmd[3])
		local k, v = next(cmd, args_start_idx)
		while k do
			append(args, json.decode(v))
			k, v = next(cmd, k)
		end

		local sref = nil
		local fref = nil
		local fname = cmd[2]
		local colon_idx = string.find(fname, ':')
		local result
		if colon_idx > 0 then
			sref = STORAGES[string.sub(fname, 1, colon_idx-1)]
			fref = sref[string.sub(fname, colon_idx+1)]
			result = fref(sref, table.unpack(args))
		else
			fref = _G[fname]
			result = fref(args)
		end
		-- log.info(result)
		-- local status, err = pcall(cmd[2], id, data, ttl + floor(clock.time()));
		if result == true then
			self:OutString("OK")
		else
			self:OutAny(result)
		end

	elseif cmd_name == 'PING' then -- https://valkey.io/commands/ping/
		if #cmd > 1 then
			self:OutText(cmd[2]);
		else
			self:OutString('PONG');
		end

	elseif cmd_name == 'AUTH' then  -- https://valkey.io/commands/auth/
		local user_name = cmd[2]
		local user_pass = cmd[3]
		if #cmd < 3 then
			user_name = 'guest'
			user_pass = cmd[2]
		end
		local user_cfg = config:get('credentials.users.'..user_name)
		if user_cfg.password == user_pass then
			self.user_cfg = user_cfg
			self.authenticated = true
			self:OutString('OK');
		else
			self:OutErrorString("Unauthorized")
		end

	elseif cmd_name == 'SELECT' then  -- https://valkey.io/commands/select/
		local next_storage_name = 'CACHE'
		if tonumber(cmd[2]) > 0 then
			next_storage_name = 'cache_'..cmd[2]
			if not STORAGES[next_storage_name] then
				STORAGES[next_storage_name] = CACHE:ForkForTableSpace(next_storage_name)
			end
		end
		self.STORAGE = STORAGES[next_storage_name]

		if self.STORAGE then
			self:OutString('OK');
		else
			self:OutErrorString("New storage is undefined.")
		end

	elseif cmd_name == 'CONFIG' then
		if cmd[2] == 'GET' then  -- https://valkey.io/commands/config-get/
			if cmd[3] == 'maxmemory' then
				self:OutList({ 'maxmemory', tostring(Get_memtx_max_tuple_size()) })
			end
		end

	elseif cmd_name == 'INFO' then -- https://valkey.io/commands/info/
		-- this command returns config as text
		local info = "# Server\r\n"
					.. 'redis_version:' .. GetTarantoolVersion() .. "\r\n"
					.. 'server_name:tarantool' .. "\r\n"
		self:OutText(info);

	elseif cmd_name == 'SET' then  -- https://valkey.io/commands/set/
		-- collect args
		local ttl = 600
		local nx = false
		local xx = false
		local key, value = next(cmd, nil)
		while key do
			if value == 'nx' then
				nx = true
			elseif value == 'xx' then
				xx = true
			elseif value == 'ex' then
				key, value = next(cmd, key)
				ttl = value
			end

			key, value = next(cmd, key)
		end

		local result
		if nx then
			result = self.STORAGE:Add(cmd[2], cmd[3], ttl)
		elseif xx then
			result = self.STORAGE:Update(cmd[2], cmd[3], ttl)
		else
			result = self.STORAGE:Set(cmd[2], cmd[3], ttl)
		end

		if result then
			self:OutString('OK');
		else
			self:OutNil();
		end

	elseif cmd_name == 'SETEX' then  -- https://valkey.io/commands/setex/
		local result = self.STORAGE:Set(cmd[2], cmd[4], cmd[3])
		if result then
			self:OutString('OK');
		else
			self:OutNil();
		end

	elseif cmd_name == 'DEL' then  -- https://valkey.io/commands/del/
		for idx, key in ipairs(cmd) do
			self.STORAGE:Delete(key)
		end

	elseif cmd_name == 'EXPIREAT' then  -- https://valkey.io/commands/expireat/
		self.STORAGE:SetTtl(cmd[2], cmd[3] - clock.time())

	--[[
	-- TODO: Need transaction per user, but not global
	elseif cmd_name == 'MULTI' then  -- https://valkey.io/commands/multi/
		box.begin()

	elseif cmd_name == 'EXEC' then  -- https://valkey.io/commands/exec/
		box.commit()

	elseif cmd_name == 'DISCARD' then  -- https://valkey.io/commands/discard/
		box.rollback()
	--]]

	--[[
	-- TODO: queue-functions
	elseif cmd_name == 'PUBLISH' then

	elseif cmd_name == 'SUBSCRIBE' then

	elseif cmd_name == 'UNSUBSCRIBE' then
	--]]

	elseif cmd_name == 'RESET' then  -- https://valkey.io/commands/reset/
		-- box.rollback()
		Redis:Command{ 'SELECT', '0' }

	elseif cmd_name == 'QUIT' then  -- https://valkey.io/commands/quit/
		self.disconnect = true

	elseif cmd_name == "LPUSH" then
		for i = 3, #cmd do
			local result = self.STORAGE:ADD_in_queu(cmd[2],cmd[i],'left',600)
			if not result then
				log.info('error in queue name or data')
				return nil
			end
		end
		self:OutString('OK')
		
	elseif cmd_name == "RPUSH" then 
		for i = 3, #cmd do
			local result = self.STORAGE:ADD_in_queu(cmd[2],cmd[i],'right',600)
			if not result then
				log.info('error in queue name or data')
				return nil
			end
		end
		self:OutString('OK')

	elseif cmd_name == 'LINDEX' then 
		self:OutText(self.STORAGE:Get_in_queue(cmd[2],cmd[3]));

	elseif cmd_name == 'LPOP' then 
		self:OutText(self.STORAGE:Delete_from_queue(cmd[2],'left'))

	elseif cmd_name == 'RPOP' then 
		self:OutText(self.STORAGE:Delete_from_queue(cmd[2],'right'))

	else
		self:OutErrorString("Not supported command: " .. cmd[1])
	end
end

--
-- https://redis.io/docs/latest/develop/reference/protocol-spec/

function Redis:OutAny(arg)
	-- log.info('< return:')
	-- log.info(arg)
	local arg_type = type(arg)
	if arg_type == 'nil' then
		return self:OutNil()
	elseif arg_type == 'boolean' then
		return self:OutBool(arg)
	elseif arg_type == 'number' then
		return self:OutNumber(arg)
	elseif arg_type == 'string' then
		return self:OutText(arg)  -- text is more universal
	elseif arg_type == 'table' then
		return self:OutTable(arg)
	end
end

function Redis:Output(chunk)
	append(self.output, chunk)
end

function Redis:FlushOutput()
	local buff = self.output
	self.output = {}
	return buff
end

function Redis:OutNil()
	self:Output('$-1')
end

function Redis:OutBool(arg)
	self:Output('#' .. (arg and 't' or 'f'))
end

function Redis:OutNumber(arg)
	if arg % 1 == 0 then
		self:Output(':' .. arg)  -- int
	else
		self:Output(',' .. arg)  -- double
	end
end

function Redis:OutErrorString(arg)
	self:Output('-' .. arg)
end

function Redis:OutString(arg)
	self:Output('+' .. arg)
end

function Redis:OutText(arg)
	if arg then
		self:Output('$' .. #arg)
		self:Output(arg)
	else
		self:OutNil()
	end
end

function Redis:OutErrorText(arg)
	if arg then
		self:Output('!' .. #arg)
		self:Output(arg)
	else
		self:OutNil()
	end
end

function Redis:OutList(arg)
	self:Output('*' .. #arg)  -- list
	for idx, value in ipairs(arg) do
		self:OutAny(value)
	end
end

function Redis:OutTable(arg)
	local cnt = 0
	for key in pairs(arg) do
		cnt = cnt + 1
	end

	self:Output('%' .. cnt)  -- map
	for key, value in pairs(arg) do
		self:OutAny(key)
		self:OutAny(value)
	end
end

#!/usr/bin/env tarantool

local strict = require('strict')
strict.on()

local fiber = require('fiber')
local clock = require('clock')
local box = require('box')
local log = require('log')

local schema = {
	structure = {
		engine = "vinyl",
		field_count = 3,
		format = {
			{ name = "id",		type = "string" },
			{ name = "data",	type = "string" },
			{ name = "ttl",		type = "unsigned" }
		},
		if_not_exists = true
	},

	indexes = {
		primary = {
			parts = { 'id' },
			unique = true,
			if_not_exists = true
		},
		ttl = {
			type = 'tree',
			parts = { 'ttl' },
			unique = false,
			if_not_exists = true
		}
	}
}

-- Create sessions storage (box.space.sessions)
local sessions_space = box.schema.space.create('sessions', schema.structure)
for iname, iparams in pairs(schema.indexes) do
	sessions_space:create_index(iname, iparams)
end

-- local tools
local add_tuple = function(id, data, ttl)
	sessions_space:insert{id, data, ttl}
end
local floor = math.floor
--

local SESSIONS = {
	space = sessions_space
}

function SESSIONS:Add(id, data, ttl)
	local status, err = pcall(add_tuple, id, data, ttl + floor(clock.time()));
	if err then
		log.info('Error: ' .. err)  -- err:unpack();
	end
	return status
end

function SESSIONS:Update(id, data, ttl)
	local result = self.space:update(
		id,
		{
			{'=', 'data', data},
			{'=', 'ttl', ttl + floor(clock.time())}
		}
	)
	return result and true or false
end

function SESSIONS:Get(id)
	local tuple = self.space:get{id};
	if tuple ~= nil and tuple.ttl > clock.time() then
		return tuple.data
	end
	return nil
end

function SESSIONS:Delete(key)
	return self.space:delete{key}
end

function SESSIONS:GC(limit)
	-- return box.execute([[ DELETE FROM "sessions" WHERE "ttl" < :now ]], {{ [':now']=floor(clock.time()) }})
	local now = floor(clock.time())
	for _, tuple in self.space.index.ttl:pairs(now, box.index.LE):take_n(limit) do
		self:Delete(tuple.id)
	end
end

function SESSIONS:Init()
	fiber.create(function() while true do self:GC(100); fiber.sleep(60) end; end)
end
SESSIONS:Init()

return SESSIONS

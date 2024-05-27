#!/usr/bin/env tarantool

local strict = require('strict')
strict.on()

local fiber = require('fiber')
local clock = require('clock')
local box = require('box')
local log = require('log')

local CACHE = {
	schema = {
		structure = {
			engine = "memtx",
			field_count = 3,
			format = {
				{ name = "key",		type = "string" },
				{ name = "value",	type = "string" },
				{ name = "ttl",		type = "unsigned", is_nullable=true },  -- ttl = box.NULL
			},
			if_not_exists = true
		},

		indexes = {
			primary = {
				parts = { 'key' },
				unique = true,
				if_not_exists = true
			},
			ttl = {
				type = 'tree',
				parts = { {'ttl', exclude_null = true} },
				unique = false,
				if_not_exists = true
			}
		}
	},

	space = {}
}

function CACHE:PrepareSpaceByName(name)
	-- Create cache space (box.space[name])
	if not box.space[name] then
		local cache_space = box.schema.space.create(name, self.schema.structure)
		for iname, iparams in pairs(self.schema.indexes) do
			cache_space:create_index(iname, iparams)
		end
	end
	return box.space[name]
end
CACHE.space = CACHE:PrepareSpaceByName('cache');

-- local tools
local add_tuple = function(key, value, ttl)
	CACHE.space:insert{key, value, ttl}
end
local floor = math.floor
--

function CACHE:Add(key, value, ttl)
	local status, err = pcall(add_tuple, key, value, ttl + floor(clock.time()));
	if err then
		log.info('Error: ' .. err)  -- err:unpack();
	end
	return status
end

function CACHE:Update(key, value, ttl)
	local result = self.space:update(
		key,
		{
			{'=', 'value', value},
			{'=', 'ttl', ttl + floor(clock.time())}
		}
	)
	return result and true or false
end

function CACHE:Set(key, value, ttl)
	return self.space:put{key, value, ttl + floor(clock.time())}
end

function CACHE:SetList(key_values, ttl)
	ttl = ttl + floor(clock.time())
	for key, value in pairs(key_values) do
		self.space:put{key, value, ttl}
	end
end

function CACHE:SetTtl(key, ttl)
	return self.space:update{key, { {'=', 'ttl', ttl + floor(clock.time())} } }
end

function CACHE:Get(key)
	local tuple = self.space:get{key};
	if tuple ~= nil and (tuple.ttl == nil or tuple.ttl > clock.time()) then
		return tuple.value
	end
	return nil
end

function CACHE:GetList(keys)
	local values_list = {}
	for key in pairs(keys) do
		values_list[key] = self:Get(key)
	end
	return values_list
end

function CACHE:Delete(key)
	return self.space:delete{key}
end

function CACHE:ForkForTableSpace(name)
	self:PrepareSpaceByName(name)

	local forkOfCache = {}
	setmetatable(forkOfCache, { __index=CACHE })  -- undefined attr => look at CACHE (prototype-like)
	forkOfCache.space = box.space[name]
	forkOfCache:Init()

	return forkOfCache
end

function CACHE:GC(limit)
	-- return box.execute([[ DELETE FROM "cache" WHERE "ttl" IS NOT NULL AND "ttl" < :now ]], {{ [':now']=floor(clock.time()) }})
	local now = floor(clock.time())
	for _, tuple in self.space.index.ttl:pairs(now, box.index.LE):take_n(limit) do
		self:Delete(tuple.key)
	end
end

function CACHE:Init()
	fiber.create(function() while true do self:GC(100); fiber.sleep(60) end; end)
end
CACHE:Init()

return CACHE

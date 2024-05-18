#!/usr/bin/env tarantool

local box = require('box')

require('init-redis-emu')
CACHE = require('init-cache')
SESSIONS = require('init-sessions')

function Get_memtx_max_tuple_size()
	return box.cfg.memtx_max_tuple_size
end

function GetTarantoolVersion()
	return box.info.version
end

local function ord(c)
    return string.format('%02X', string.byte(c))
end

function Bin2hex(str)
    return str:gsub('.', ord)
end

-- Create queue storage
-- queue = require 'queue'
-- queue.create_tube('defered_tasks', 'fifo')
-- queue.tube.defered_tasks:put('{action: "clear_old_data"}')
-- queue.tube.defered_tasks:take()

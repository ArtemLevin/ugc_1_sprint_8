local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local window = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

local current_tokens = redis.call('ZCARD', key)

if current_tokens < capacity then
    redis.call('ZADD', key, now, now)
    return 1
else
    return 0
end
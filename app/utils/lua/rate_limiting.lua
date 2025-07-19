local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local window = tonumber(ARGV[4])

local current_tokens = redis.call('ZCARD', key)

if current_tokens >= capacity then
    return 0
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest and #oldest > 0 and now - oldest[2] > window then
    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
end

redis.call('ZADD', key, now, now)
return 1
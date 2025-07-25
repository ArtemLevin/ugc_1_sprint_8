-- rate_limiting.lua
-- Lua-скрипт для реализации Leaky Bucket алгоритма в Redis
-- Используется в RedisLeakyBucketRateLimiter

local key = KEYS[1]
local rate = tonumber(ARGV[1])       -- максимальное количество запросов в окне
local capacity = tonumber(ARGV[2])    -- максимальный размер "ведра"
local now = tonumber(ARGV[3])         -- текущее время в миллисекундах
local window = tonumber(ARGV[4])      -- размер окна в миллисекундах

-- Текущее количество запросов в "ведре"
local current_tokens = redis.call('ZCARD', key)

-- Если превышено максимальное количество запросов — запрет
if current_tokens >= capacity then
    return 0
end

-- Удаляем устаревшие записи
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- Проверяем, сколько осталось после очистки
local remaining_tokens = redis.call('ZCARD', key)

-- Если есть место — добавляем текущий запрос
if remaining_tokens < capacity then
    redis.call('ZADD', key, now, now)
    return 1  -- запрос разрешён
else
    return 0  -- запрос запрещён
end
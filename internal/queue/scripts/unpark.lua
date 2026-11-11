-- Moves as many parked deliveries into the stream as the semaphore has free slots.
-- KEYS: 1 parking lot, 2 its semaphore, 3 parking index, 4 stream. ARGV: 1 limit.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local free = tonumber(ARGV[1]) - redis.call('ZCARD', KEYS[2])
local moved = 0
if free > 0 then
  local items = redis.call('ZPOPMIN', KEYS[1], free)
  for i = 1, #items, 2 do
    redis.call('XADD', KEYS[4], '*', 'i', items[i])
    moved = moved + 1
  end
end
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('SREM', KEYS[3], KEYS[1])
end
return moved

-- Frees a delivery's slots and wakes the oldest parked delivery of the endpoint and of the tenant.
-- KEYS: 1 endpoint slots, 2 tenant slots, 3 endpoint parking, 4 tenant parking, 5 parking index, 6 stream
-- ARGV: 1 delivery id
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
local moved = 0
for i = 3, 4 do
  local p = redis.call('ZPOPMIN', KEYS[i])
  if p[1] then
    redis.call('XADD', KEYS[6], '*', 'i', p[1])
    moved = moved + 1
  end
  if redis.call('ZCARD', KEYS[i]) == 0 then
    redis.call('SREM', KEYS[5], KEYS[i])
  end
end
return moved

-- Moves due delayed deliveries into the stream. KEYS: 1 delayed set, 2 stream. ARGV: 1 max.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local items = redis.call('ZRANGE', KEYS[1], '-inf', now, 'BYSCORE', 'LIMIT', 0, tonumber(ARGV[1]))
for _, it in ipairs(items) do
  redis.call('XADD', KEYS[2], '*', 'i', it)
end
if #items > 0 then
  redis.call('ZREM', KEYS[1], unpack(items))
end
return #items

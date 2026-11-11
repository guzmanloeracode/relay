-- Takes an endpoint slot, a tenant slot and a delivery token, all or nothing.
-- KEYS: 1 endpoint slots, 2 tenant slots, 3 endpoint parking, 4 tenant parking, 5 parking index,
--       6 tenant delivery bucket, 7 delayed set
-- ARGV: 1 delivery id, 2 item, 3 lease ms, 4 endpoint limit, 5 tenant limit, 6 rate, 7 burst
-- Returns {verdict, wait ms}: 0 acquired, 1 endpoint busy (parked), 2 tenant busy (parked),
-- 3 rate limited (delayed), 4 an attempt for this delivery already holds a slot.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return {4, 0}
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
  redis.call('ZADD', KEYS[3], 'NX', now, ARGV[2])
  redis.call('SADD', KEYS[5], KEYS[3])
  return {1, 0}
end
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[5]) then
  redis.call('ZADD', KEYS[4], 'NX', now, ARGV[2])
  redis.call('SADD', KEYS[5], KEYS[4])
  return {2, 0}
end
local rate, burst = tonumber(ARGV[6]), tonumber(ARGV[7])
local b = redis.call('HMGET', KEYS[6], 'tokens', 'ts')
local tokens, ts = tonumber(b[1]), tonumber(b[2])
if tokens == nil or ts == nil then tokens, ts = burst, now end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)
if tokens < 1 then
  local wait = math.ceil((1 - tokens) * 1000 / rate)
  redis.call('HSET', KEYS[6], 'tokens', tostring(tokens), 'ts', now)
  redis.call('ZADD', KEYS[7], now + wait, ARGV[2])
  return {3, wait}
end
redis.call('HSET', KEYS[6], 'tokens', tostring(tokens - 1), 'ts', now)
redis.call('PEXPIRE', KEYS[6], math.ceil(burst * 1000 / rate) + 1000)
local lease = now + tonumber(ARGV[3])
redis.call('ZADD', KEYS[1], lease, ARGV[1])
redis.call('ZADD', KEYS[2], lease, ARGV[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 2)
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]) * 2)
return {0, 0}

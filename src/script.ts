/**
 * Sliding window log, executed atomically inside Redis.
 *
 * Each key is a sorted set with one entry per accepted request, scored by its
 * timestamp in milliseconds. A request is allowed when fewer than `limit`
 * entries are left after dropping the ones older than the window.
 *
 * KEYS[1] sorted set of the client
 * ARGV[1] window size in ms
 * ARGV[2] max requests per window
 * ARGV[3] unique member for this request
 * ARGV[4] current time in ms, or "" to use the Redis server clock
 *
 * Returns { allowed (0|1), remaining, resetMs } where resetMs is the time
 * until the oldest entry leaves the window and frees a slot.
 */
export const CONSUME_SCRIPT = `
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[4])

if not now then
  local time = redis.call('TIME')
  now = time[1] * 1000 + math.floor(time[2] / 1000)
end

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

local allowed = 0
if count < limit then
  redis.call('ZADD', key, now, ARGV[3])
  redis.call('PEXPIRE', key, window)
  count = count + 1
  allowed = 1
end

local reset = window
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if oldest[2] then
  reset = tonumber(oldest[2]) + window - now
end

return { allowed, limit - count, reset }
`;

/**
 * Read-only variant: reports the state of the window without consuming.
 * Same KEYS/ARGV as above, except ARGV[3] is unused.
 */
export const PEEK_SCRIPT = `
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[4])

if not now then
  local time = redis.call('TIME')
  now = time[1] * 1000 + math.floor(time[2] / 1000)
end

local count = redis.call('ZCOUNT', key, '(' .. (now - window), '+inf')
local reset = 0
if count > 0 then
  local first = redis.call('ZRANGEBYSCORE', key, '(' .. (now - window), '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
  reset = tonumber(first[2]) + window - now
end

local allowed = 0
if count < limit then allowed = 1 end

return { allowed, math.max(limit - count, 0), reset }
`;

local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local lease_key = ARGV[3]
local lease_json = ARGV[4]
local lease_id = ARGV[5]
local lease_ttl_ms = tonumber(ARGV[6])
local now_ms = tonumber(ARGV[7])
local expires_at_ms = tonumber(ARGV[8])
local dry_run = ARGV[9] == "1"
local ops = cjson.decode(ARGV[10])
local decision_id = ARGV[11]

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  return cjson.encode(decoded)
end

local allowed = true
local statuses = {}

for i, op in ipairs(ops) do
  redis.call("ZREMRANGEBYSCORE", op.write_key, "-inf", now_ms)
  local active = tonumber(redis.call("ZCARD", op.write_key))
  local status = {
    limit_id = op.limit_id,
    used = active,
    remaining = math.max(0, op.limit - active),
    retry_after_ms = 0,
    allowed = true,
    message = "allowed"
  }
  if active + 1 > op.limit then
    status.allowed = false
    status.message = "concurrency exceeded"
    allowed = false
  elseif not dry_run then
    status.used = active + 1
    status.remaining = math.max(0, op.limit - active - 1)
  end
  statuses[i] = status
end

if allowed and not dry_run then
  for _, op in ipairs(ops) do
    redis.call("ZADD", op.write_key, expires_at_ms, lease_id)
    redis.call("PEXPIRE", op.write_key, lease_ttl_ms + 3600000)
  end
  redis.call("PSETEX", lease_key, lease_ttl_ms + 3600000, lease_json)
end

local result = cjson.encode({
  cached = false,
  decision_id = decision_id,
  allowed = allowed,
  dry_run = dry_run,
  lease_id = lease_id,
  statuses = statuses
})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

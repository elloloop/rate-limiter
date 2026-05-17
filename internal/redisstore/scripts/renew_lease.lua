local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local lease_key = ARGV[3]
local lease_id = ARGV[4]
local extend_ttl_ms = tonumber(ARGV[5])
local now_ms = tonumber(ARGV[6])

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  return cjson.encode(decoded)
end

local raw = redis.call("GET", lease_key)
if not raw then
  local missing = cjson.encode({found = false, renewed = false})
  redis.call("PSETEX", idem_key, idem_ttl_ms, missing)
  return missing
end

local lease = cjson.decode(raw)
local renewed = false
if lease.status == "LEASE_STATUS_ACTIVE" then
  local expires_at_ms = now_ms + extend_ttl_ms
  lease.expires_at_unix_ms = expires_at_ms
  if lease.impacts ~= nil then
    for _, impact in ipairs(lease.impacts) do
      redis.call("ZADD", impact.lease_set_key, expires_at_ms, lease_id)
      redis.call("PEXPIRE", impact.lease_set_key, extend_ttl_ms + 3600000)
    end
  end
  local updated = cjson.encode(lease)
  redis.call("PSETEX", lease_key, extend_ttl_ms + 3600000, updated)
  renewed = true
end

local result = cjson.encode({found = true, renewed = renewed, lease = lease})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

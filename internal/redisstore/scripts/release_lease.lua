local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local lease_key = ARGV[3]
local lease_id = ARGV[4]

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  return cjson.encode(decoded)
end

local raw = redis.call("GET", lease_key)
if not raw then
  local missing = cjson.encode({found = false, released = false})
  redis.call("PSETEX", idem_key, idem_ttl_ms, missing)
  return missing
end

local lease = cjson.decode(raw)
local released = false
if lease.status == "LEASE_STATUS_ACTIVE" then
  if lease.impacts ~= nil then
    for _, impact in ipairs(lease.impacts) do
      redis.call("ZREM", impact.lease_set_key, lease_id)
    end
  end
  lease.status = "LEASE_STATUS_RELEASED"
  local updated = cjson.encode(lease)
  local ttl = redis.call("PTTL", lease_key)
  if ttl ~= nil and tonumber(ttl) > 0 then
    redis.call("PSETEX", lease_key, ttl, updated)
  else
    redis.call("SET", lease_key, updated)
  end
  released = true
end

local result = cjson.encode({found = true, released = released, lease = lease})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

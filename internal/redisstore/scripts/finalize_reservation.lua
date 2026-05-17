local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local reservation_key = ARGV[3]
local actual_cost = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  return cjson.encode(decoded)
end

local raw = redis.call("GET", reservation_key)
if not raw then
  local missing = cjson.encode({found = false, finalized = false})
  redis.call("PSETEX", idem_key, idem_ttl_ms, missing)
  return missing
end

local res = cjson.decode(raw)
if res.status ~= "RESERVATION_STATUS_ACTIVE" then
  local current = cjson.encode({
    found = true,
    finalized = false,
    reservation = res,
    reserved_cost = tonumber(res.reserved_cost or 0),
    actual_cost = tonumber(res.actual_cost or 0),
    refunded_cost = tonumber(res.refunded_cost or 0),
    overage_cost = tonumber(res.overage_cost or 0)
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, current)
  return current
end

local reserved_cost = tonumber(res.reserved_cost or 0)
local refunded_cost = 0
local overage_cost = 0
if actual_cost < reserved_cost then
  refunded_cost = reserved_cost - actual_cost
elseif actual_cost > reserved_cost then
  overage_cost = actual_cost - reserved_cost
end

if refunded_cost > 0 and res.impacts ~= nil then
  for _, impact in ipairs(res.impacts) do
    if impact.refundable == true then
      local alg = impact.algorithm
      if alg == "ALGORITHM_FIXED_WINDOW_CALENDAR" or alg == "ALGORITHM_FIXED_WINDOW_DURATION" or alg == "ALGORITHM_SLIDING_WINDOW" then
        local after = redis.call("DECRBY", impact.redis_key, refunded_cost)
        if tonumber(after) < 0 then
          redis.call("SET", impact.redis_key, "0")
        end
      elseif alg == "ALGORITHM_TOKEN_BUCKET" or alg == "ALGORITHM_LEAKY_BUCKET" then
        redis.call("HINCRBYFLOAT", impact.redis_key, "tokens", refunded_cost)
      end
    end
  end
end

res.actual_cost = actual_cost
res.refunded_cost = refunded_cost
res.overage_cost = overage_cost
res.finalized_at_unix_ms = now_ms
res.status = "RESERVATION_STATUS_FINALIZED"

local updated = cjson.encode(res)
local ttl = redis.call("PTTL", reservation_key)
if ttl ~= nil and tonumber(ttl) > 0 then
  redis.call("PSETEX", reservation_key, ttl, updated)
else
  redis.call("SET", reservation_key, updated)
end

local result = cjson.encode({
  found = true,
  finalized = true,
  reservation = res,
  reserved_cost = reserved_cost,
  actual_cost = actual_cost,
  refunded_cost = refunded_cost,
  overage_cost = overage_cost
})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

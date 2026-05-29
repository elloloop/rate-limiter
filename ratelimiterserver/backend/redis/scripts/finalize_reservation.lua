local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local reservation_key = ARGV[3]
local reservation_index_key = ARGV[4]
local actual_cost = tonumber(ARGV[5])
local now_ms = tonumber(ARGV[6])

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
local reserved_cost = tonumber(res.reserved_cost or 0)

local function max(a, b)
  if a > b then return a end
  return b
end

local function capacity(impact)
  local cap = tonumber(impact.burst or 0)
  if cap == nil or cap <= 0 then cap = tonumber(impact.limit or 0) end
  return cap
end

local function preserve_set(key, value)
  local ttl = redis.call("PTTL", key)
  if ttl ~= nil and tonumber(ttl) > 0 then
    redis.call("PSETEX", key, ttl, value)
  else
    redis.call("SET", key, value)
  end
end

local function refund_impact(impact, amount, respect_refundable, require_refund_full)
  if respect_refundable and impact.refundable ~= true then
    return false
  end
  if require_refund_full and impact.expiry_policy ~= "RESERVATION_EXPIRY_POLICY_REFUND_FULL" then
    return false
  end
  local alg = impact.algorithm
  if alg == "ALGORITHM_FIXED_WINDOW_CALENDAR" or alg == "ALGORITHM_FIXED_WINDOW_DURATION" or alg == "ALGORITHM_SLIDING_WINDOW" then
    local after = redis.call("DECRBY", impact.redis_key, amount)
    if tonumber(after) < 0 then
      redis.call("SET", impact.redis_key, "0")
    end
    return true
  elseif alg == "ALGORITHM_TOKEN_BUCKET" or alg == "ALGORITHM_LEAKY_BUCKET" then
    local cap = capacity(impact)
    local after = tonumber(redis.call("HINCRBYFLOAT", impact.redis_key, "tokens", amount))
    if cap > 0 and after > cap then
      redis.call("HSET", impact.redis_key, "tokens", cap)
    end
    return true
  elseif alg == "ALGORITHM_GCRA" then
    local rate = tonumber(impact.refill_rate_per_sec or 0)
    if rate ~= nil and rate > 0 then
      local tat = tonumber(redis.call("GET", impact.redis_key) or "0")
      local next_tat = max(0, tat - ((amount / rate) * 1000))
      preserve_set(impact.redis_key, tostring(next_tat))
      return true
    end
  end
  return false
end

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

if tonumber(res.expires_at_unix_ms or 0) <= now_ms then
  local refunded_cost = 0
  if res.impacts ~= nil then
    for _, impact in ipairs(res.impacts) do
      if refund_impact(impact, reserved_cost, false, true) then
        refunded_cost = reserved_cost
      end
    end
  end
  res.refunded_cost = refunded_cost
  res.finalized_at_unix_ms = now_ms
  res.status = "RESERVATION_STATUS_EXPIRED"
  preserve_set(reservation_key, cjson.encode(res))
  redis.call("ZREM", reservation_index_key, reservation_key)
  local expired = cjson.encode({
    found = true,
    finalized = false,
    reservation = res,
    reserved_cost = reserved_cost,
    actual_cost = tonumber(res.actual_cost or 0),
    refunded_cost = refunded_cost,
    overage_cost = tonumber(res.overage_cost or 0)
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, expired)
  return expired
end

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
      refund_impact(impact, refunded_cost, true, false)
    end
  end
end

res.actual_cost = actual_cost
res.refunded_cost = refunded_cost
res.overage_cost = overage_cost
res.finalized_at_unix_ms = now_ms
res.status = "RESERVATION_STATUS_FINALIZED"

local updated = cjson.encode(res)
preserve_set(reservation_key, updated)
redis.call("ZREM", reservation_index_key, reservation_key)

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

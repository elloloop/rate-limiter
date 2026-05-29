local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local reservation_key = ARGV[3]
local reservation_index_key = ARGV[4]
local delta_cost = tonumber(ARGV[5])
local now_ms = tonumber(ARGV[6])
local decision_id = ARGV[7]

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  if decoded.decision ~= nil then
    decoded.decision.cached = true
  end
  return cjson.encode(decoded)
end

local function max(a, b)
  if a > b then return a end
  return b
end

local function min(a, b)
  if a < b then return a end
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

local function refund_impact(impact, amount, respect_refundable)
  if respect_refundable and impact.refundable ~= true then
    return
  end
  local alg = impact.algorithm
  if alg == "ALGORITHM_FIXED_WINDOW_CALENDAR" or alg == "ALGORITHM_FIXED_WINDOW_DURATION" or alg == "ALGORITHM_SLIDING_WINDOW" then
    local after = redis.call("DECRBY", impact.redis_key, amount)
    if tonumber(after) < 0 then
      redis.call("SET", impact.redis_key, "0")
    end
  elseif alg == "ALGORITHM_TOKEN_BUCKET" or alg == "ALGORITHM_LEAKY_BUCKET" then
    local cap = capacity(impact)
    local after = tonumber(redis.call("HINCRBYFLOAT", impact.redis_key, "tokens", amount))
    if cap > 0 and after > cap then
      redis.call("HSET", impact.redis_key, "tokens", cap)
    end
  elseif alg == "ALGORITHM_GCRA" then
    local rate = tonumber(impact.refill_rate_per_sec or 0)
    if rate ~= nil and rate > 0 then
      local tat = tonumber(redis.call("GET", impact.redis_key) or "0")
      local next_tat = max(0, tat - ((amount / rate) * 1000))
      preserve_set(impact.redis_key, tostring(next_tat))
    end
  end
end

local function decision(allowed, message, statuses, retry_after_ms)
  return {
    cached = false,
    decision_id = decision_id,
    allowed = allowed,
    retry_after_ms = retry_after_ms or 0,
    statuses = statuses or {},
    message = message
  }
end

local raw = redis.call("GET", reservation_key)
if not raw then
  local result = cjson.encode({
    found = false,
    active = false,
    reserved_cost = 0,
    decision = decision(false, "reservation not found", {}, 0)
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, result)
  return result
end

local res = cjson.decode(raw)
local reserved_cost = tonumber(res.reserved_cost or 0)

if res.status ~= "RESERVATION_STATUS_ACTIVE" then
  local result = cjson.encode({
    found = true,
    active = false,
    reserved_cost = reserved_cost,
    decision = decision(false, "reservation is not active", {}, 0),
    reservation = res
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, result)
  return result
end

if tonumber(res.expires_at_unix_ms or 0) <= now_ms then
  local refunded_cost = 0
  if res.impacts ~= nil then
    for _, impact in ipairs(res.impacts) do
      if impact.expiry_policy == "RESERVATION_EXPIRY_POLICY_REFUND_FULL" then
        refund_impact(impact, reserved_cost, false)
        refunded_cost = reserved_cost
      end
    end
  end
  res.refunded_cost = refunded_cost
  res.status = "RESERVATION_STATUS_EXPIRED"
  res.finalized_at_unix_ms = now_ms
  local updated = cjson.encode(res)
  preserve_set(reservation_key, updated)
  redis.call("ZREM", reservation_index_key, reservation_key)
  local result = cjson.encode({
    found = true,
    active = false,
    reserved_cost = reserved_cost,
    decision = decision(false, "reservation expired", {}, 0),
    reservation = res
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, result)
  return result
end

if delta_cost == 0 then
  local result = cjson.encode({
    found = true,
    active = true,
    reserved_cost = reserved_cost,
    decision = decision(true, "reservation unchanged", {}, 0),
    reservation = res
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, result)
  return result
end

local new_reserved_cost = reserved_cost + delta_cost
if new_reserved_cost < 0 then
  local result = cjson.encode({
    found = true,
    active = true,
    reserved_cost = reserved_cost,
    decision = decision(false, "delta_cost would make reservation negative", {}, 0),
    reservation = res
  })
  redis.call("PSETEX", idem_key, idem_ttl_ms, result)
  return result
end

local statuses = {}
local allowed = true
local retry_after_ms = 0
local next_tokens = {}
local next_tats = {}

if delta_cost > 0 and res.impacts ~= nil then
  for i, impact in ipairs(res.impacts) do
    local status = {
      limit_id = impact.limit_id,
      used = 0,
      remaining = 0,
      retry_after_ms = 0,
      allowed = true,
      message = "allowed"
    }
    local alg = impact.algorithm
    local limit = tonumber(impact.limit or 0)
    if alg == "ALGORITHM_FIXED_WINDOW_CALENDAR" or alg == "ALGORITHM_FIXED_WINDOW_DURATION" or alg == "ALGORITHM_SLIDING_WINDOW" then
      local current = tonumber(redis.call("GET", impact.redis_key) or "0")
      local next_used = current + delta_cost
      if limit <= 0 or next_used > limit then
        status.allowed = false
        status.message = "limit exceeded"
        status.retry_after_ms = max(0, tonumber(impact.reset_at_unix_ms or 0) - now_ms)
        status.used = current
        status.remaining = max(0, limit - current)
        allowed = false
        retry_after_ms = max(retry_after_ms, status.retry_after_ms)
      else
        status.used = next_used
        status.remaining = max(0, limit - next_used)
      end
    elseif alg == "ALGORITHM_TOKEN_BUCKET" or alg == "ALGORITHM_LEAKY_BUCKET" then
      local cap = capacity(impact)
      local rate = tonumber(impact.refill_rate_per_sec or 0)
      if cap <= 0 or rate <= 0 then
        status.allowed = false
        status.message = "missing bucket parameters"
        allowed = false
      else
        local state = redis.call("HMGET", impact.redis_key, "tokens", "last_refill_ms")
        local tokens = tonumber(state[1])
        local last_refill_ms = tonumber(state[2])
        if tokens == nil then tokens = cap end
        if last_refill_ms == nil then last_refill_ms = now_ms end
        local elapsed_sec = max(0, now_ms - last_refill_ms) / 1000
        local available = min(cap, tokens + (elapsed_sec * rate))
        if available < delta_cost then
          local deficit = delta_cost - available
          status.allowed = false
          status.message = "bucket exhausted"
          status.retry_after_ms = math.ceil((deficit / rate) * 1000)
          status.used = math.floor(cap - available)
          status.remaining = math.floor(available)
          allowed = false
          retry_after_ms = max(retry_after_ms, status.retry_after_ms)
        else
          next_tokens[i] = available - delta_cost
          status.used = math.floor(cap - next_tokens[i])
          status.remaining = math.floor(next_tokens[i])
        end
      end
    elseif alg == "ALGORITHM_GCRA" then
      local rate = tonumber(impact.refill_rate_per_sec or 0)
      local burst = capacity(impact)
      if rate <= 0 or burst <= 0 then
        status.allowed = false
        status.message = "missing gcra parameters"
        allowed = false
      else
        local tat = tonumber(redis.call("GET", impact.redis_key) or "0")
        local interval_ms = (delta_cost / rate) * 1000
        local tolerance_ms = (burst / rate) * 1000
        local earliest_ms = tat - tolerance_ms
        if now_ms < earliest_ms then
          status.allowed = false
          status.message = "rate exceeded"
          status.retry_after_ms = math.ceil(earliest_ms - now_ms)
          status.used = 1
          status.remaining = 0
          allowed = false
          retry_after_ms = max(retry_after_ms, status.retry_after_ms)
        else
          next_tats[i] = max(now_ms, tat) + interval_ms
          status.used = 0
          status.remaining = 1
        end
      end
    else
      status.allowed = false
      status.message = "unsupported reservation impact"
      allowed = false
    end
    statuses[i] = status
  end
end

if allowed then
  if res.impacts ~= nil then
    for i, impact in ipairs(res.impacts) do
      local alg = impact.algorithm
      if delta_cost > 0 then
        if alg == "ALGORITHM_FIXED_WINDOW_CALENDAR" or alg == "ALGORITHM_FIXED_WINDOW_DURATION" or alg == "ALGORITHM_SLIDING_WINDOW" then
          redis.call("INCRBY", impact.redis_key, delta_cost)
        elseif alg == "ALGORITHM_TOKEN_BUCKET" or alg == "ALGORITHM_LEAKY_BUCKET" then
          redis.call("HSET", impact.redis_key, "tokens", next_tokens[i], "last_refill_ms", now_ms)
        elseif alg == "ALGORITHM_GCRA" then
          preserve_set(impact.redis_key, tostring(next_tats[i]))
        end
      else
        refund_impact(impact, 0 - delta_cost, true)
      end
      impact.reserved_cost = new_reserved_cost
    end
  end
  res.reserved_cost = new_reserved_cost
  local updated = cjson.encode(res)
  preserve_set(reservation_key, updated)
end

local result = cjson.encode({
  found = true,
  active = true,
  reserved_cost = res.reserved_cost,
  decision = decision(allowed, allowed and "reservation incremented" or "increment reservation denied", statuses, retry_after_ms),
  reservation = res
})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

local reservation_index_key = ARGV[1]
local now_ms = tonumber(ARGV[2])
local batch_size = tonumber(ARGV[3])

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

local function refund_impact(impact, amount)
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

local members = redis.call("ZRANGEBYSCORE", reservation_index_key, "-inf", now_ms, "LIMIT", 0, batch_size)
local expired = 0

for _, reservation_key in ipairs(members) do
  redis.call("ZREM", reservation_index_key, reservation_key)
  local raw = redis.call("GET", reservation_key)
  if raw then
    local res = cjson.decode(raw)
    if res.status == "RESERVATION_STATUS_ACTIVE" and tonumber(res.expires_at_unix_ms or 0) <= now_ms then
      local refunded_cost = 0
      local reserved_cost = tonumber(res.reserved_cost or 0)
      if res.impacts ~= nil then
        for _, impact in ipairs(res.impacts) do
          if impact.expiry_policy == "RESERVATION_EXPIRY_POLICY_REFUND_FULL" then
            refund_impact(impact, reserved_cost)
            refunded_cost = reserved_cost
          end
        end
      end
      res.refunded_cost = refunded_cost
      res.finalized_at_unix_ms = now_ms
      res.status = "RESERVATION_STATUS_EXPIRED"
      preserve_set(reservation_key, cjson.encode(res))
      expired = expired + 1
    end
  end
end

return cjson.encode({expired = expired, scanned = #members})

local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local reservation_key = ARGV[3]
local now_ms = tonumber(ARGV[4])

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  return cjson.encode(decoded)
end

local raw = redis.call("GET", reservation_key)
if not raw then
  local missing = cjson.encode({found = false, released = false, released_cost = 0})
  redis.call("PSETEX", idem_key, idem_ttl_ms, missing)
  return missing
end

local res = cjson.decode(raw)
local released = false
local released_cost = 0

if res.status == "RESERVATION_STATUS_ACTIVE" then
  released = true
  released_cost = tonumber(res.reserved_cost or 0)
  if res.impacts ~= nil then
    for _, impact in ipairs(res.impacts) do
      if impact.refundable == true then
        local alg = impact.algorithm
        if alg == "ALGORITHM_FIXED_WINDOW_CALENDAR" or alg == "ALGORITHM_FIXED_WINDOW_DURATION" or alg == "ALGORITHM_SLIDING_WINDOW" then
          local after = redis.call("DECRBY", impact.redis_key, released_cost)
          if tonumber(after) < 0 then
            redis.call("SET", impact.redis_key, "0")
          end
        elseif alg == "ALGORITHM_TOKEN_BUCKET" or alg == "ALGORITHM_LEAKY_BUCKET" then
          redis.call("HINCRBYFLOAT", impact.redis_key, "tokens", released_cost)
        end
      end
    end
  end
  res.refunded_cost = released_cost
  res.finalized_at_unix_ms = now_ms
  res.status = "RESERVATION_STATUS_RELEASED"
  local updated = cjson.encode(res)
  local ttl = redis.call("PTTL", reservation_key)
  if ttl ~= nil and tonumber(ttl) > 0 then
    redis.call("PSETEX", reservation_key, ttl, updated)
  else
    redis.call("SET", reservation_key, updated)
  end
end

local result = cjson.encode({
  found = true,
  released = released,
  released_cost = released_cost,
  reservation = res
})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

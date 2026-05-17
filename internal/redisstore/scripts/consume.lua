local idem_key = ARGV[1]
local idem_ttl_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local dry_run = ARGV[4] == "1"
local ops = cjson.decode(ARGV[5])
local decision_id = ARGV[6]

local cached = redis.call("GET", idem_key)
if cached then
  local decoded = cjson.decode(cached)
  decoded.cached = true
  return cjson.encode(decoded)
end

local allowed = true
local retry_after_ms = 0
local statuses = {}

local function max(a, b)
  if a > b then return a end
  return b
end

local function min(a, b)
  if a < b then return a end
  return b
end

for i, op in ipairs(ops) do
  local status = {
    limit_id = op.limit_id,
    used = 0,
    remaining = 0,
    retry_after_ms = 0,
    allowed = true,
    message = "allowed"
  }

  if op.kind == "counter" then
    local current = 0
    for _, key in ipairs(op.read_keys) do
      current = current + tonumber(redis.call("GET", key) or "0")
    end
    local next_used = current + op.cost
    if next_used > op.limit then
      status.allowed = false
      status.message = "limit exceeded"
      status.retry_after_ms = max(0, op.reset_at_unix_ms - now_ms)
      allowed = false
      retry_after_ms = max(retry_after_ms, status.retry_after_ms)
      status.used = current
      status.remaining = max(0, op.limit - current)
    else
      status.used = next_used
      status.remaining = max(0, op.limit - next_used)
    end
  elseif op.kind == "token_bucket" or op.kind == "leaky_bucket" then
    local capacity = op.burst
    if capacity == nil or capacity <= 0 then capacity = op.limit end
    local state = redis.call("HMGET", op.write_key, "tokens", "last_refill_ms")
    local tokens = tonumber(state[1])
    local last_refill_ms = tonumber(state[2])
    if tokens == nil then tokens = capacity end
    if last_refill_ms == nil then last_refill_ms = now_ms end
    local elapsed_sec = max(0, now_ms - last_refill_ms) / 1000
    local available = min(capacity, tokens + (elapsed_sec * op.refill_rate_per_sec))
    if available < op.cost then
      local deficit = op.cost - available
      status.allowed = false
      status.message = "bucket exhausted"
      status.retry_after_ms = math.ceil((deficit / op.refill_rate_per_sec) * 1000)
      allowed = false
      retry_after_ms = max(retry_after_ms, status.retry_after_ms)
      status.used = math.floor(capacity - available)
      status.remaining = math.floor(available)
      op.next_tokens = available
    else
      op.next_tokens = available - op.cost
      status.used = math.floor(capacity - op.next_tokens)
      status.remaining = math.floor(op.next_tokens)
    end
  elseif op.kind == "gcra" then
    local tat = tonumber(redis.call("GET", op.write_key) or "0")
    local interval_ms = (op.cost / op.refill_rate_per_sec) * 1000
    local burst = op.burst
    if burst == nil or burst <= 0 then burst = op.limit end
    local tolerance_ms = (burst / op.refill_rate_per_sec) * 1000
    local earliest_ms = tat - tolerance_ms
    if now_ms < earliest_ms then
      status.allowed = false
      status.message = "rate exceeded"
      status.retry_after_ms = math.ceil(earliest_ms - now_ms)
      allowed = false
      retry_after_ms = max(retry_after_ms, status.retry_after_ms)
      status.used = 1
      status.remaining = 0
      op.next_tat = tat
    else
      local base = max(now_ms, tat)
      op.next_tat = base + interval_ms
      status.used = 0
      status.remaining = 1
    end
  else
    status.allowed = false
    status.message = "unsupported operation kind"
    allowed = false
  end

  statuses[i] = status
end

if allowed and not dry_run then
  for _, op in ipairs(ops) do
    if op.kind == "counter" then
      redis.call("INCRBY", op.write_key, op.cost)
      redis.call("PEXPIRE", op.write_key, op.ttl_ms)
    elseif op.kind == "token_bucket" or op.kind == "leaky_bucket" then
      redis.call("HSET", op.write_key, "tokens", op.next_tokens, "last_refill_ms", now_ms)
      redis.call("PEXPIRE", op.write_key, op.ttl_ms)
    elseif op.kind == "gcra" then
      redis.call("SET", op.write_key, tostring(op.next_tat), "PX", op.ttl_ms)
    end
  end
elseif dry_run then
  for i, op in ipairs(ops) do
    if statuses[i].allowed and op.kind == "counter" then
      local current = 0
      for _, key in ipairs(op.read_keys) do
        current = current + tonumber(redis.call("GET", key) or "0")
      end
      statuses[i].used = current
      statuses[i].remaining = max(0, op.limit - current)
    end
  end
end

local result = cjson.encode({
  cached = false,
  decision_id = decision_id,
  allowed = allowed,
  dry_run = dry_run,
  retry_after_ms = retry_after_ms,
  statuses = statuses
})
redis.call("PSETEX", idem_key, idem_ttl_ms, result)
return result

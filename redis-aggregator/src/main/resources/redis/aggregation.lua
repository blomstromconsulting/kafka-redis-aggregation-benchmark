#!lua name=aggbench
--[[
  Atomic aggregation state transitions for variant B (Redis Functions).

  This is the Lua twin of io.aggbench.domain.AggregationLogic and must classify every delivery
  identically; both are tested against the same fixtures (ArrivalScenarios).

  State of one correlation id = one hash at KEYS[1]:
    run, corr, created, completed, s2 (0/1), fin (0/1), dup, stale, late
    <t>:id <t>:v <t>:p <t>:h <t>:inf <t>:ts <t>:ref <t>:trig   for t in a, b, c
    sup:<eventId> = "<ref>|<triggers>"                         superseded versions (redelivery detection)
  Open (incomplete) aggregates = sorted set at KEYS[2], score = created ms, member = correlation id.

  Retention (same values as the Kafka Streams variant):
    incomplete aggregates expire incomplete-ttl after creation (key PEXPIRE at creation),
    completed aggregates expire completed-retention after completion (PEXPIRE at completion).
]]

local TYPES = { 'a', 'b', 'c' }
local STAGE2, FINAL = 1, 2

local function same_event(key, stored_ref, stored_triggers, ref)
  if stored_ref == ref then
    if stored_triggers > 0 then
      return { 'REDELIVERED', stored_triggers, redis.call('HGETALL', key) }
    end
    return { 'REDELIVERED', 0 }
  end
  redis.call('HINCRBY', key, 'dup', 1)
  return { 'DUPLICATE', 0 }
end

-- KEYS: aggregate hash, open sorted set
-- ARGV: runId, correlationId, type(1-3), eventId, version, payload, payloadHash, requiresInference(0/1),
--       appendMs, ref(partition:offset), nowMs, incompleteTtlMs, completedRetentionMs
local function merge(keys, args)
  local key, open_key = keys[1], keys[2]
  local corr, t, event_id = args[2], TYPES[tonumber(args[3])], args[4]
  local version, ref, now = tonumber(args[5]), args[10], tonumber(args[11])

  if redis.call('EXISTS', key) == 0 then
    redis.call('HSET', key, 'run', args[1], 'corr', corr, 'created', now, 'completed', 0,
               's2', 0, 'fin', 0, 'dup', 0, 'stale', 0, 'late', 0)
    redis.call('PEXPIRE', key, args[12])
    redis.call('ZADD', open_key, now, corr)
  end

  local existing_id = redis.call('HGET', key, t .. ':id')
  if existing_id then
    if existing_id == event_id then
      return same_event(key, redis.call('HGET', key, t .. ':ref'), tonumber(redis.call('HGET', key, t .. ':trig')), ref)
    end
    local sup = redis.call('HGET', key, 'sup:' .. event_id)
    if sup then
      local sep = string.find(sup, '|', 1, true)
      return same_event(key, string.sub(sup, 1, sep - 1), tonumber(string.sub(sup, sep + 1)), ref)
    end
    if version <= tonumber(redis.call('HGET', key, t .. ':v')) then
      redis.call('HINCRBY', key, 'stale', 1)
      return { 'STALE', 0 }
    end
    local s2 = redis.call('HGET', key, 's2') == '1'
    local fin = redis.call('HGET', key, 'fin') == '1'
    if fin or (s2 and t ~= 'c') then
      redis.call('HINCRBY', key, 'late', 1)
      return { 'LATE_VERSION', 0 }
    end
    redis.call('HSET', key, 'sup:' .. existing_id,
               redis.call('HGET', key, t .. ':ref') .. '|' .. redis.call('HGET', key, t .. ':trig'))
  end

  redis.call('HSET', key, t .. ':id', event_id, t .. ':v', version, t .. ':p', args[6], t .. ':h', args[7],
             t .. ':inf', args[8], t .. ':ts', args[9], t .. ':ref', ref, t .. ':trig', 0)

  local has_ab = redis.call('HEXISTS', key, 'a:id') == 1 and redis.call('HEXISTS', key, 'b:id') == 1
  local has_c = redis.call('HEXISTS', key, 'c:id') == 1
  local triggered = 0
  if has_ab and redis.call('HGET', key, 's2') ~= '1' then
    triggered = triggered + STAGE2
    redis.call('HSET', key, 's2', 1)
  end
  if has_ab and has_c and redis.call('HGET', key, 'fin') ~= '1' then
    triggered = triggered + FINAL
    redis.call('HSET', key, 'fin', 1, 'completed', now)
    redis.call('ZREM', open_key, corr)
    redis.call('PEXPIRE', key, args[13])
  end
  redis.call('HSET', key, t .. ':trig', triggered)

  local outcome = existing_id and 'SUPERSEDED' or 'ACCEPTED'
  if triggered > 0 then
    return { outcome, triggered, redis.call('HGETALL', key) }
  end
  return { outcome, 0 }
end

-- Removes open-set entries older than the cutoff (their hashes already expired by TTL) and returns how many
-- this call removed, so that concurrent sweepers on several pods never double count.
-- KEYS: open sorted set   ARGV: cutoffMs, limit
local function sweep_expired(keys, args)
  local expired = redis.call('ZRANGEBYSCORE', keys[1], '-inf', args[1], 'LIMIT', 0, tonumber(args[2]))
  if #expired == 0 then
    return 0
  end
  return redis.call('ZREM', keys[1], unpack(expired))
end

redis.register_function('aggbench_merge', merge)
redis.register_function('aggbench_sweep_expired', sweep_expired)

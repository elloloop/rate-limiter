# Redis Model

V1 supports Redis single-primary or primary-replica topology. Redis Cluster is
explicitly out of scope because one quota decision may touch multiple arbitrary
keys, and Redis Cluster cannot run one Lua script across arbitrary hash slots.

## Prefix

All keys use:

```text
quota:v1:{environment}:{product}:
```

## Keys

```text
quota:v1:{env}:{product}:req:{request_id}
quota:v1:{env}:{product}:fw:{limit_id_hash}:{scope_hash}:{window_id}
quota:v1:{env}:{product}:sw:{limit_id_hash}:{scope_hash}:{bucket_id}
quota:v1:{env}:{product}:tb:{limit_id_hash}:{scope_hash}
quota:v1:{env}:{product}:lb:{limit_id_hash}:{scope_hash}
quota:v1:{env}:{product}:gcra:{limit_id_hash}:{scope_hash}
quota:v1:{env}:{product}:res:{reservation_id}
quota:v1:{env}:{product}:lease_set:{limit_id_hash}:{scope_hash}
quota:v1:{env}:{product}:lease:{lease_id}
```

`limit_id` and `scope_key` are hashed in keys to avoid unsafe Redis key
characters and to keep key length bounded.

## Required Scripts

```text
consume.lua
reserve.lua
finalize_reservation.lua
release_reservation.lua
acquire_lease.lua
renew_lease.lua
release_lease.lua
```

All writes must go through these scripts.


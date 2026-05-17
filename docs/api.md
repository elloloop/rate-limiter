# API

The protobuf contract lives at `quota/v1/quota.proto`.

The v1 service surface is:

```proto
service QuotaService {
  rpc Consume(ConsumeRequest) returns (ConsumeResponse);
  rpc Reserve(ReserveRequest) returns (ReserveResponse);
  rpc FinalizeReservation(FinalizeReservationRequest) returns (FinalizeReservationResponse);
  rpc ReleaseReservation(ReleaseReservationRequest) returns (ReleaseReservationResponse);
  rpc AcquireLease(AcquireLeaseRequest) returns (AcquireLeaseResponse);
  rpc RenewLease(RenewLeaseRequest) returns (RenewLeaseResponse);
  rpc ReleaseLease(ReleaseLeaseRequest) returns (ReleaseLeaseResponse);
  rpc Explain(ExplainRequest) returns (ExplainResponse);
  rpc GetCurrentUsage(GetCurrentUsageRequest) returns (GetCurrentUsageResponse);
  rpc ValidateLimits(ValidateLimitsRequest) returns (ValidateLimitsResponse);
  rpc GetReservation(GetReservationRequest) returns (Reservation);
  rpc GetLease(GetLeaseRequest) returns (Lease);
  rpc GetRedisStatus(GetRedisStatusRequest) returns (RedisStatus);
}
```

## Request IDs

All mutating operations require `request_id`. The same `request_id` returns the
same stored result instead of applying a second mutation.

Recommended TTLs:

- `Consume`: 24 hours
- `Reserve`: reservation TTL plus 24 hours
- `FinalizeReservation` and `ReleaseReservation`: 24 hours
- lease operations: lease TTL plus 1 hour

## Decision

Every enforcement call returns a `Decision`:

- `allowed`
- `reason`
- `retry_after_ms`
- `limit_statuses`
- metadata such as idempotency hits

`LimitStatus` is the caller-facing usage snapshot for each supplied limit.

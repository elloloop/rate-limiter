# Examples

## Workspace Email Recipients

```json
{
  "request_id": "req_email_123",
  "context": {
    "product": "workspace",
    "environment": "prod"
  },
  "action": "workspace.email.recipients",
  "cost": 25,
  "limits": [
    {
      "limit_id": "user_email_recipients_daily",
      "scope_key": "user:user_123",
      "action": "workspace.email.recipients",
      "unit": "recipients",
      "algorithm": "ALGORITHM_FIXED_WINDOW_CALENDAR",
      "window": {
        "type": "WINDOW_TYPE_CALENDAR",
        "calendar_unit": "CALENDAR_UNIT_DAY",
        "timezone": "UTC"
      },
      "limit": 500
    }
  ]
}
```

## Assistant Token Reservation

Use `Reserve` before the model call and `FinalizeReservation` after the call
with the actual token count. Finalization uses the stored reservation impact
keys, not the current wall-clock window.

## Assistant Concurrency

Use `AcquireLease` before an operation and `ReleaseLease` when it finishes.
Renew long-running operations before `lease_ttl_ms` expires.


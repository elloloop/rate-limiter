# Deployment

The service is distributed as a Docker image:

```text
ghcr.io/elloloop/rate-limiter
```

## Local Docker

```bash
docker run --rm -p 8080:8080 -p 9090:9090 \
  -e QUOTA_PRODUCT=workspace \
  -e QUOTA_ENVIRONMENT=local \
  -e QUOTA_REDIS_URL=redis://host.docker.internal:6379/0 \
  ghcr.io/elloloop/rate-limiter:latest
```

## Kubernetes

Run one deployment per product or environment. Do not share Redis counters
between products unless the product explicitly owns that sharing boundary.

Expose gRPC only inside trusted networks. Use TLS or mTLS when the runtime
network requires it.

## Release

Release tags publish:

- multi-arch Docker image
- GitHub Release notes
- protobuf source archive
- updated GitHub Pages docs


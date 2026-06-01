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
  ghcr.io/elloloop/rate-limiter:0.4.7
```

Use a version tag or digest for every deployment. The mutable `latest` tag is
published for discovery, not for reproducible rollouts.

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
- release-time docs build verification

The release workflow will not publish the image until Redis-backed Go tests and
the Docker Compose critical RPC e2e test pass.

GitHub Pages deploys from `main` through the docs workflow.

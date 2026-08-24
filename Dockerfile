# Rate Limiter service image.
#
# Build:
#   docker build --target server -t rate-limiter .
#
# Run:
#   docker run -p 8080:8080 -p 9090:9090 \
#     -e QUOTA_REDIS_URL=redis://redis:6379/0 \
#     rate-limiter

FROM --platform=$BUILDPLATFORM golang:1.27.0-alpine3.23 AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY gen/ ./gen/
COPY internal/ ./internal/
COPY quota/ ./quota/
COPY ratelimiterserver/ ./ratelimiterserver/
COPY VERSION ./

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build \
      -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT" \
      -o /bin/quota-service \
      ./cmd/quota-service

FROM scratch AS server

COPY --from=builder /bin/quota-service /bin/quota-service
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY examples/ /app/examples/
COPY VERSION /app/VERSION

EXPOSE 8080 9090

USER 65532:65532
ENTRYPOINT ["/bin/quota-service"]
CMD ["serve"]

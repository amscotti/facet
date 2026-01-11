# Build stage
FROM crystallang/crystal:latest-alpine AS builder

WORKDIR /app

# Copy dependency files first for better caching
COPY shard.yml shard.lock* ./

# Install dependencies
RUN shards install --production

# Copy source code
COPY src/ src/

# Build static binary
RUN crystal build src/facet.cr -o facet --release --static --no-debug && \
    strip facet

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/facet .

# Create non-root user for security
RUN addgroup -S facet && adduser -S facet -G facet
USER facet

# Expose Redis default port
EXPOSE 6379

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD echo "PING" | nc localhost 6379 | grep -q "PONG" || exit 1

ENTRYPOINT ["./facet"]

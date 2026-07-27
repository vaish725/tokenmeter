# Multi-stage build producing a minimal, cgo-free binary in a FROM scratch
# final image. FROM scratch ships zero CA certificates, and this binary
# makes outbound HTTPS calls to the Anthropic/OpenAI APIs - skip the
# certificate copy below and every upstream call fails TLS verification.
FROM golang:1.26 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/meter ./cmd/meter

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/meter /meter
COPY configs /configs

# meter.db defaults to ./meter.db (working directory) - mount a volume at
# /data and set METER_DB_PATH=/data/meter.db to persist it across restarts.
EXPOSE 8080 8081

ENTRYPOINT ["/meter"]

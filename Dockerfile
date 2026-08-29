# --- Build stage ---
FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache git

COPY go.mod ./
RUN go mod download 2>/dev/null || true

COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/routing-engine .

# --- Runtime stage ---
FROM alpine:3.19

RUN apk add --no-cache ca-certificates && \
    addgroup -S dptrb && adduser -S dptrb -G dptrb

WORKDIR /app
COPY --from=builder /out/routing-engine /app/routing-engine

USER dptrb

ENTRYPOINT ["/app/routing-engine"]

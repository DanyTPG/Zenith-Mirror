FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o zenith-mirror .

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/zenith-mirror /app/zenith-mirror

ENTRYPOINT ["/app/zenith-mirror"]
CMD ["-config", "/app/config.json"]

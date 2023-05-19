FROM golang:1.20-alpine as builder
WORKDIR /server
ENV CGO_ENABLED 0
ENV GOOS linux

RUN apk update && apk add --no-cache git ca-certificates upx && update-ca-certificates

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-w -s" -o binary  ./cmd
    #&& upx --brute binary

# Runtime
FROM scratch
WORKDIR /
ARG TZ
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /server/binary /
COPY --from=builder /usr/local/go/lib/time/zoneinfo.zip /
ENV TZ=${TZ:-Europe/Stockholm}
ENV ZONEINFO=/zoneinfo.zip

ENTRYPOINT ["./binary"]

# Build a static zuuld binary and ship it in a minimal distroless image.
# golang:1.26 tracks the latest 1.26.x patch (>= 1.26.4), matching go.mod and
# carrying the stdlib security fixes. The build is hermetic: it uses the committed
# vendor/ directory (go's default when vendor/ is present), so no network is needed.
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /zuuld ./cmd/zuuld

FROM gcr.io/distroless/static:nonroot
COPY --from=build /zuuld /zuuld
# Raft transport and client/forwarding gRPC.
EXPOSE 9001 8001
ENTRYPOINT ["/zuuld"]

# syntax=docker/dockerfile:1
# Relay: one static binary on a distroless base. The same image runs `relay api`, `relay worker` and
# `relay migrate`.
FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X github.com/guzmanloeracode/relay/internal/config.Version=${VERSION} -X github.com/guzmanloeracode/relay/internal/config.Commit=${COMMIT}" \
      -o /out/relay ./cmd/relay

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/relay /usr/local/bin/relay
USER nonroot:nonroot
EXPOSE 8080 9091
ENTRYPOINT ["/usr/local/bin/relay"]
CMD ["api"]

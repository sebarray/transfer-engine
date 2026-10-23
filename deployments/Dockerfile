FROM golang:1.27.3-alpine3.24@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS build

WORKDIR /src
ENV GOTOOLCHAIN=local CGO_ENABLED=0

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY docs/embed.go docs/openapi.json ./docs/
RUN go build -mod=readonly -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -o /out/api ./cmd/api

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/api /api
COPY migrations/ /migrations/

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/api"]

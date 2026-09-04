# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.25.2 AS build

ARG TARGETOS
ARG TARGETARCH
WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags='-s -w -extldflags "-static"' \
    -o /out/kube-imagerelease-operator \
    ./cmd

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/kube-imagerelease-operator /kube-imagerelease-operator
USER 65532:65532
ENTRYPOINT ["/kube-imagerelease-operator"]

FROM golang:1.25 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN useradd -u 10001 bento

WORKDIR /go/src/github.com/warpstreamlabs/bento/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/warpstreamlabs/bento/
RUN go mod download

# Build
COPY . /go/src/github.com/warpstreamlabs/bento/
# Tag timetzdata required for busybox base image:
# https://github.com/warpstreamlabs/bento/issues/897
RUN make TAGS="timetzdata"

# Pack
FROM busybox AS package

LABEL maintainer="WarpStream Labs <security@warpstreamlabs.com>"
LABEL org.opencontainers.image.source="https://github.com/warpstreamlabs/bento"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/warpstreamlabs/bento/target/bin/bento .
COPY ./config/docker.yaml /bento.yaml

USER bento

EXPOSE 4195

ENTRYPOINT ["/bento"]

CMD ["-c", "/bento.yaml"]

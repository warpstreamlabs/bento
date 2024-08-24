ARG BUILD_PLATFORM=linux/amd64

FROM --platform=$BUILD_PLATFORM golang:1.22 AS build

ENV CGO_ENABLED=1
ENV CGO_LDFLAGS="-L/usr/lib/"
ENV GOOS="linux"
ENV GOARCH="amd64"
ENV TAGS="huggingbento"

WORKDIR /go/src/github.com/warpstreamlabs/bento/
# Update dependencies: On unchanged dependencies, cached layer will be reused
COPY go.* /go/src/github.com/warpstreamlabs/bento/
RUN go mod download

COPY --from=ghcr.io/knights-analytics/hugot:latest /usr/lib/libtokenizers.a /usr/lib/libtokenizers.a
COPY --from=ghcr.io/knights-analytics/hugot:latest /usr/lib64/onnxruntime.so /usr/lib/onnxruntime.so
# Build
COPY . /go/src/github.com/warpstreamlabs/bento/

RUN make huggingbento

# Pack
FROM --platform=$BUILD_PLATFORM public.ecr.aws/amazonlinux/amazonlinux:2023

LABEL maintainer="WarpStream Labs <security@warpstreamlabs.com>"
LABEL org.opencontainers.image.source="https://github.com/warpstreamlabs/bento"

WORKDIR /root/

COPY --from=build /usr/lib/libtokenizers.a /usr/lib/libtokenizers.a
COPY --from=build /usr/lib/onnxruntime.so /usr/lib/onnxruntime.so

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /go/src/github.com/warpstreamlabs/bento/target/bin/huggingbento .
COPY ./config/docker.yaml /bento.yaml

EXPOSE 4195

ENTRYPOINT ["./huggingbento"]

CMD ["-c", "/bento.yaml"]

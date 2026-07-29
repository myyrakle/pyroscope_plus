# Self-contained multi-arch image build: compiles the UI and the Go
# binaries inside the image, so no host-side preparation is needed.
#
#   docker buildx build --push --platform linux/amd64,linux/arm64 \
#     -t <registry>/<image>:<tag> -f cmd/pyroscope/standalone.Dockerfile .
#
# Both stages run on the build host's native platform ($BUILDPLATFORM) and
# cross-compile for $TARGETARCH, so no QEMU emulation is involved.

FROM --platform=$BUILDPLATFORM node:24@sha256:bb20cf73b3ad7212834ec48e2174cdcb5775f6550510a5336b842ae32741ce6c AS ui
WORKDIR /pyroscope/ui
COPY ui/package.json ui/yarn.lock ui/.yarnrc.yml ui/.npmrc ./
RUN corepack enable && yarn install --immutable
COPY ui/index.html ui/vite.config.ts ./
COPY ui/tsconfig*.json ./
COPY ui/src ./src
COPY ui/public ./public
RUN yarn build

FROM --platform=$BUILDPLATFORM golang:1.25 AS builder
WORKDIR /pyroscope
COPY . .
COPY --from=ui /pyroscope/ui/dist ui/dist
ARG TARGETOS TARGETARCH
# GOAMD64 is only consulted when TARGETARCH is amd64.
ENV GOOS=$TARGETOS GOARCH=$TARGETARCH GOAMD64=v2 CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -tags "netgo embedassets" -ldflags '-extldflags "-static" -s -w' -o /out/pyroscope ./cmd/pyroscope && \
    go build -ldflags '-extldflags "-static" -s -w' -o /out/profilecli ./cmd/profilecli

FROM gcr.io/distroless/static:debug@sha256:7dc183cc0aea6abd9d105135e49d37b7474a79391ebea7eb55557cd4486d2225 AS debug

SHELL [ "/busybox/sh", "-c" ]

RUN addgroup -g 10001 -S pyroscope && \
    adduser -u 10001 -S pyroscope -G pyroscope -h /data

FROM gcr.io/distroless/static@sha256:87bce11be0af225e4ca761c40babb06d6d559f5767fbf7dc3c47f0f1a466b92c

COPY --from=debug /etc/passwd /etc/passwd
COPY --from=debug /etc/group /etc/group

# Copy folder from debug container, this folder needs to have the correct UID
# in order for the container to run as non-root.
VOLUME /data
COPY --chown=pyroscope:pyroscope --from=debug /data /data
VOLUME /data-compactor
COPY --chown=pyroscope:pyroscope --from=debug /data /data-compactor
VOLUME /data-metastore
COPY --chown=pyroscope:pyroscope --from=debug /data /data-metastore

COPY cmd/pyroscope/pyroscope.yaml /etc/pyroscope/config.yaml
COPY --from=builder /out/profilecli /usr/bin/profilecli
COPY --from=builder /out/pyroscope /usr/bin/pyroscope

USER pyroscope
EXPOSE 4040
ENTRYPOINT [ "/usr/bin/pyroscope" ]
CMD ["-config.file=/etc/pyroscope/config.yaml"]

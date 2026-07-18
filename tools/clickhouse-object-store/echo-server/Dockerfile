FROM golang:1.25.12-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN GOWORK=off go mod download
COPY . ./
RUN GOWORK=off CGO_ENABLED=0 go build -trimpath -o /out/echo-server .

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

RUN addgroup -S -g 10001 app && adduser -S -D -H -u 10001 -G app app
COPY --from=builder /out/echo-server /usr/local/bin/echo-server

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/echo-server"]

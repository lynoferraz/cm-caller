# syntax=docker.io/docker/dockerfile:1.4
ARG CM_VERSION=0.19-preview2
ARG NONODO_VERSION=2.10.1-beta
ARG CM_CALLER_VERSION=0.2.0
ARG TRAEFIK_VERSION=3.1.6
ARG S6_OVERLAY_VERSION=3.2.0.2

FROM debian:11-slim as base

RUN <<EOF
apt-get update && \
apt-get install -y --no-install-recommends wget ca-certificates xz-utils && \
rm -rf /var/lib/apt/lists/* /var/log/* /var/cache/*
EOF

ARG CM_VERSION
RUN wget -qO- https://github.com/edubart/cartesi-machine-everywhere/releases/download/v${CM_VERSION}/cartesi-machine-linux-musl-$(dpkg --print-architecture).tar.xz | \
    tar xJf - --strip-components 1 -C / cartesi-machine-linux-musl-$(dpkg --print-architecture)/bin cartesi-machine-linux-musl-$(dpkg --print-architecture)/share

ARG NONODO_VERSION
RUN wget -qO- https://github.com/Calindra/nonodo/releases/download/v${NONODO_VERSION}/nonodo-v${NONODO_VERSION}-linux-$(dpkg --print-architecture).tar.gz | \
    tar xzf - -C /usr/local/bin nonodo

ARG CM_CALLER_VERSION
RUN wget -qO- https://github.com/lynoferraz/cm-caller/releases/download/v${CM_CALLER_VERSION}/cm-caller-v${CM_CALLER_VERSION}-linux-$(dpkg --print-architecture).tar.gz | \
    tar xzf - -C /usr/local/bin cm-caller


FROM base as node-base

ARG TRAEFIK_VERSION
RUN wget -qO- https://github.com/traefik/traefik/releases/download/v${TRAEFIK_VERSION}/traefik_v${TRAEFIK_VERSION}_linux_$(dpkg --print-architecture).tar.gz | \
    tar xzf - -C /usr/local/bin traefik

ARG S6_OVERLAY_VERSION
RUN wget -qO- https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz | \
    tar xJf - -C / 
RUN wget -qO- https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-$(uname -m).tar.xz | \
    tar xJf - -C / 
# RUN rm -rf /etc/s6-overlay/s6-rc.d/*

RUN mkdir -p /data
RUN mkdir -p /data-inspect

# Configure traefik
RUN <<EOF
mkdir -p /etc/traefik
echo '
entryPoints:
  web:
    address: ":80"
  anvil:
    address: ":8545"
http:
  routers:
    inspect-router:
      rule: "PathPrefix(`/inspect`)"
      service: inspect-service
    advance-router:
      rule: "PathPrefix(`/graphql`)"
      service: advance-service
    anvil-router:
      entryPoints:
        - "anvil"
      service: anvil-service
  services:
    inspect-service:
      loadBalancer:
        servers:
          - url: "http://localhost:8081/inspect"
    advance-service:
      loadBalancer:
        servers:
          - url: "http://localhost:8080/graphql"
    anvil-service:
      loadBalancer:
        servers:
          - url: "http://localhost:8546/"
' > /etc/traefik/traefik.yaml
EOF

# Configure s6 services
RUN <<EOF
mkdir -p /etc/s6-overlay/s6-rc.d/advance
echo "longrun" > /etc/s6-overlay/s6-rc.d/advance/type
echo "#!/bin/sh
exec nonodo --http-rollups-port=5004 --http-port=8080 --anvil-port=8546 --disable-inspect -- cm-caller -image=/mnt/snapshots/0 -store-path=/data -disable-inspect -disable-consistency-checks -disable-remote
" > /etc/s6-overlay/s6-rc.d/advance/run
mkdir -p /etc/s6-overlay/s6-rc.d/inspect/dependencies.d
touch /etc/s6-overlay/s6-rc.d/inspect/dependencies.d/advance
echo "longrun" > /etc/s6-overlay/s6-rc.d/inspect/type
echo "#!/bin/sh
exec nonodo --http-rollups-port=5005 --http-port=8081 --disable-devnet --disable-advance -- cm-caller -image=/mnt/snapshots/0 -store-path=/data-inspect -enable-watcher -watcher-path=/data/latest -disable-advance -disable-consistency-checks -reset-latest -disable-remote
" > /etc/s6-overlay/s6-rc.d/inspect/run
mkdir -p /etc/s6-overlay/s6-rc.d/traefik/dependencies.d
touch /etc/s6-overlay/s6-rc.d/traefik/dependencies.d/advance \
    /etc/s6-overlay/s6-rc.d/traefik/dependencies.d/inspect
echo "longrun" > /etc/s6-overlay/s6-rc.d/traefik/type
echo "#!/bin/sh
exec traefik
" > /etc/s6-overlay/s6-rc.d/traefik/run
mkdir -p /etc/s6-overlay/s6-rc.d/user/contents.d
touch /etc/s6-overlay/s6-rc.d/user/contents.d/advance \
    /etc/s6-overlay/s6-rc.d/user/contents.d/inspect \
    /etc/s6-overlay/s6-rc.d/user/contents.d/traefik
EOF

FROM node-base as node

COPY image /mnt/snapshots/0

# TODO: remove this after nonodo version update
COPY nonodo /usr/local/bin/nonodo

CMD ["/init"]

# syntax=docker.io/docker/dockerfile:1.4
ARG CM_VERSION=0.19-preview2
ARG NONODO_VERSION=2.11.0-beta
ARG CM_CALLER_VERSION=0.2.0-rc.1
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

RUN useradd --user-group app

# Configure traefik
RUN <<EOF
mkdir -p /etc/traefik
echo '
entryPoints:
  web:
    address: ":80"
    asDefault: true
  anvil:
    address: ":8545"
providers:
  file:
    filename: /etc/traefik/dynamic_conf.yaml
' > /etc/traefik/traefik.yaml
echo '
http:
  routers:
    inspect-router:
      rule: "PathPrefix(`/inspect`)"
      service: inspect-service
    advance-router:
      rule: "PathPrefix(`/graphql`)"
      service: advance-service
    anvil-router:
      rule: "PathPrefix(`/`)"
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
' > /etc/traefik/dynamic_conf.yaml
EOF

RUN mkdir -p -m 777 /mnt/snapshots
ARG IMAGE_SNAPSHOT_PATH=/mnt/snapshots/0
ENV IMAGE_SNAPSHOT_PATH ${IMAGE_SNAPSHOT_PATH}
ARG DATA_PATH=/mnt/node
ENV DATA_PATH ${DATA_PATH}

# Configure s6 services
RUN <<EOF
mkdir -p /etc/s6-overlay/s6-rc.d/prepare-dirs
echo "oneshot" > /etc/s6-overlay/s6-rc.d/prepare-dirs/type
echo "#!/command/with-contenv sh
mkdir -p \${DATA_PATH}/db
mkdir -p \${DATA_PATH}/advance
mkdir -p \${DATA_PATH}/inspect
" > /etc/s6-overlay/s6-rc.d/prepare-dirs/run.sh
chmod +x /etc/s6-overlay/s6-rc.d/prepare-dirs/run.sh
echo "/etc/s6-overlay/s6-rc.d/prepare-dirs/run.sh" \
> /etc/s6-overlay/s6-rc.d/prepare-dirs/up
mkdir -p /etc/s6-overlay/s6-rc.d/advance/dependencies.d
touch /etc/s6-overlay/s6-rc.d/advance/dependencies.d/prepare-dirs
echo "longrun" > /etc/s6-overlay/s6-rc.d/advance/type
echo "#!/command/with-contenv sh
nonodo_chain_args='--anvil-port=8546'
if [ ! -z \"\${FROM_BLOCK}\" ] && [ ! -z \"\${RPC_URL}\" ] && [ ! -z \"\${APP_ADDRESS}\" ]; then
  nonodo_chain_args=\"--from-block=\${FROM_BLOCK} --contracts-input-box-block=\${FROM_BLOCK} --rpc-url=\${RPC_URL} --contracts-application-address=\${APP_ADDRESS}\"
fi
exec nonodo \
  --http-rollups-port=5004 --http-port=8080 \
  --sqlite-file=\${DATA_PATH}/db/database.sqlite \
  \${nonodo_chain_args} \
  --disable-inspect -- \
  cm-caller \
    -image=\${IMAGE_SNAPSHOT_PATH} \
    -store-path=\${DATA_PATH}/advance \
    -disable-inspect -disable-consistency-checks -disable-remote
" > /etc/s6-overlay/s6-rc.d/advance/run
mkdir -p /etc/s6-overlay/s6-rc.d/inspect/dependencies.d
touch /etc/s6-overlay/s6-rc.d/inspect/dependencies.d/advance
touch /etc/s6-overlay/s6-rc.d/inspect/dependencies.d/prepare-dirs
echo "longrun" > /etc/s6-overlay/s6-rc.d/inspect/type
echo "#!/command/with-contenv sh
exec nonodo \
  --http-rollups-port=5005 \
  --http-port=8081 \
  --disable-devnet --disable-advance -- \
  cm-caller \
    -image=\${IMAGE_SNAPSHOT_PATH} \
    -store-path=\${DATA_PATH}/inspect \
    -enable-watcher -watcher-path=\${DATA_PATH}/advance/local_image \
    -disable-advance -disable-consistency-checks -disable-remote
" > /etc/s6-overlay/s6-rc.d/inspect/run
mkdir -p /etc/s6-overlay/s6-rc.d/traefik
echo "longrun" > /etc/s6-overlay/s6-rc.d/traefik/type
echo "#!/bin/sh
exec traefik
" > /etc/s6-overlay/s6-rc.d/traefik/run
mkdir -p /etc/s6-overlay/s6-rc.d/user/contents.d
touch /etc/s6-overlay/s6-rc.d/user/contents.d/traefik \
    /etc/s6-overlay/s6-rc.d/user/contents.d/prepare-dirs \
    /etc/s6-overlay/s6-rc.d/user/contents.d/advance \
    /etc/s6-overlay/s6-rc.d/user/contents.d/inspect
EOF


FROM node-base as node

USER app

CMD ["/init"]

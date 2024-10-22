# syntax=docker.io/docker/dockerfile:1.4
ARG CM_VERSION=0.19-preview2
ARG NONODO_VERSION=2.10.1-beta
ARG CM_CALLER_VERSION=0.2.0
ARG TRAEFIK_VERSION=3.1.6
ARG S6_OVERLAY_VERSION=

FROM debian:11-slim as base

RUN <<EOF
apt-get update && \
apt-get install -y --no-install-recommends wgetca-certificates xz-utils && \
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


FROM base as node

ARG TRAEFIK_VERSION
RUN wget -qO- https://github.com/traefik/traefik/releases/download/v${TRAEFIK_VERSION}/traefik_v${TRAEFIK_VERSION}_linux_$(dpkg --print-architecture).tar.gz | \
    tar xzf - -C /usr/local/bin traefik

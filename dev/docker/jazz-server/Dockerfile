FROM rust:1.93.1-bookworm AS builder

RUN apt-get update \
  && apt-get install --yes --no-install-recommends clang cmake libclang-dev pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /source
COPY . .

RUN cargo build \
  --locked \
  --release \
  --package jazz-cli \
  --bin jazz-tools \
  --features otel

FROM debian:bookworm-slim AS runtime

ARG VCS_REF=unknown
LABEL org.opencontainers.image.source="https://github.com/garden-co/jazz" \
  org.opencontainers.image.revision="${VCS_REF}" \
  org.opencontainers.image.title="Jazz server" \
  org.opencontainers.image.description="Native Jazz server image for JazzHost"

RUN apt-get update \
  && apt-get install --yes --no-install-recommends ca-certificates libstdc++6 \
  && rm -rf /var/lib/apt/lists/* \
  && groupadd --gid 10001 jazz \
  && useradd --uid 10001 --gid jazz --no-create-home --shell /usr/sbin/nologin jazz \
  && install --directory --owner jazz --group jazz /data

COPY --from=builder /source/target/release/jazz-tools /usr/local/bin/jazz-tools

ENV NODE_ENV=production
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
EXPOSE 1625

ENTRYPOINT ["/usr/local/bin/jazz-tools"]
CMD ["--help"]

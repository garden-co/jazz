FROM rust:1.93.1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples

RUN cargo build --release -p jazz-cloud-server

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/jazz-cloud-server /usr/local/bin/jazz-cloud-server

ENTRYPOINT ["/usr/local/bin/jazz-cloud-server"]

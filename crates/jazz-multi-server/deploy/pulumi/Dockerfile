FROM rust:1.86-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples/todo-server-rs ./examples/todo-server-rs

RUN cargo build --release -p jazz-multi-server

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/jazz-multi-server /usr/local/bin/jazz-multi-server

ENTRYPOINT ["/usr/local/bin/jazz-multi-server"]

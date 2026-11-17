FROM rust:1.99.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ENV LIBSQLITE3_FLAGS="SQLITE_ENABLE_BATCH_ATOMIC_WRITE SQLITE_DIRECT_OVERFLOW_READ=0"
RUN cargo build --locked --release --manifest-path crates/jazz-rivet/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/crates/jazz-rivet/target/release/jazz-rivet /usr/local/bin/jazz-rivet
USER 65532:65532
ENV PORT=8080 RUST_LOG=info
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/jazz-rivet"]

# Builds the jazz-ingest benchmark and an LD_PRELOAD write-delay shim so the
# benchmark can be run against a simulated slow disk.
#   docker build -f dev/benchmarks/jazz-ingest/docker/Dockerfile -t jazz-ingest-bench .
#   docker run --rm -e LD_PRELOAD=/usr/local/lib/writedelay.so -e WRITE_DELAY_NS=2000000 \
#     jazz-ingest-bench /src/target/release/jazz-ingest-bench --raw rocksdb,slatedb
FROM rust:1.93-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev cmake git && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# The jazz crate is memory-heavy; cap parallelism so the build fits the Docker
# VM's RAM (the repo's own gates use -j 2 for the same reason).
ENV CARGO_BUILD_JOBS=2
RUN cargo build --release -j 2 -p jazz-ingest-bench

RUN clang -O2 -fPIC -shared -o /usr/local/lib/writedelay.so \
    dev/benchmarks/jazz-ingest/docker/delay.c -ldl

ENTRYPOINT ["/bin/bash", "-lc"]

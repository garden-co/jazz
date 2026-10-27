# Both digests identify multi-architecture indexes (x64 and ARM64).
FROM rust:1.93.1-bullseye@sha256:fae8ebcb8eda28d37df2b965c87c15a572dac2b6d3893a72d6dea99d700d1786 AS rust
FROM node:24.13.0-bullseye@sha256:8036dbe5b1f465e3acb8b866031cd06e4f84c31b0e83dabbdc59397a40dbe288
ARG TARGETARCH
COPY --from=rust /usr/local/rustup/ /usr/local/rustup/
COPY --from=rust /usr/local/cargo/bin/ /usr/local/cargo/bin/
# Bullseye (glibc 2.31, GCC 10) is the oldest userspace the binding builds on;
# the node image already provides gcc, g++, make and git. Bullseye's apt
# repositories move under a pinned image, so CMake (needed by mimalloc) comes
# from a checksummed Kitware release instead.
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) arch=x86_64; sha256=312d78e0b7c5c9b4a97a87a46c3e525aa84895fe8135c238c4616bba73dc9518 ;; \
      arm64) arch=aarch64; sha256=09b7127b09e3d3ed5fa894f4e59f6a01112f267da6acc4249e46dba97b14afcb ;; \
      *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl --fail --location --silent --show-error --output /tmp/cmake.tar.gz \
      "https://github.com/Kitware/CMake/releases/download/v3.31.9/cmake-3.31.9-linux-$arch.tar.gz"; \
    echo "$sha256  /tmp/cmake.tar.gz" | sha256sum --check --strict; \
    tar -xzf /tmp/cmake.tar.gz --strip-components=1 -C /usr/local; \
    rm /tmp/cmake.tar.gz; \
    npm install --global pnpm@10.14.0
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH
ENV CC=gcc CXX=g++

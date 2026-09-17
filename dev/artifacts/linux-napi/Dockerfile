# All three digests identify multi-architecture indexes (x64 and ARM64).
FROM node:24.13.0-bullseye@sha256:8036dbe5b1f465e3acb8b866031cd06e4f84c31b0e83dabbdc59397a40dbe288 AS node
FROM rust:1.93.1-bullseye@sha256:fae8ebcb8eda28d37df2b965c87c15a572dac2b6d3893a72d6dea99d700d1786 AS rust
FROM amazonlinux:2023@sha256:065856aabb1ddab0441f4024dfbba116cc760286858a53ec74e6a1218af979ee
COPY --from=node /usr/local/ /usr/local/
COPY --from=rust /usr/local/rustup/ /usr/local/rustup/
COPY --from=rust /usr/local/cargo/bin/ /usr/local/cargo/bin/
# AL2023 pins its package repository release through /etc/dnf/vars/releasever.
# GCC11 is needed by RocksDB's C++20 `using enum`; Bullseye GCC10 cannot build it.
RUN dnf install -y gcc gcc-c++ clang-devel cmake make git gzip tar findutils \
 && dnf clean all \
 && npm install --global pnpm@10.14.0
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH
ENV CC=gcc CXX=g++

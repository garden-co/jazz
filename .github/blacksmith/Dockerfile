FROM ubuntu:24.04@sha256:019e8eb29a85e74d64925745884f2ec79aa27e3feab36353d24656f4d6b89467

ARG NODE_VERSION=24.13.0
ARG PNPM_VERSION=10.14.0
ARG RUST_VERSION=1.93.1
ARG SCCACHE_VERSION=0.15.0
ARG CARGO_NEXTEST_VERSION=0.9.143
ARG WASM_PACK_VERSION=0.13.1
ARG WASM_BINDGEN_VERSION=0.2.117
ARG PLAYWRIGHT_VERSION=1.58.2
ARG NODE_SHA256=e798599612f4bb71333a3397ab0d095fd62214e115aea45aa858a145fc72d67e
ARG RUSTUP_INIT_SHA256=4acc9acc76d5079515b46346a485974457b5a79893cfb01112423c89aeb5aa10
ARG SCCACHE_SHA256=782d2b5dd7ae0a55ebe368ab258114d0928d019ac2d949ab85d5d02f3926709e
ARG CARGO_NEXTEST_SHA256=66786b9abe23920d022a182d1416b1bbc8130dd4872a9553d76985a1708dcd1e
ARG WASM_PACK_SHA256=c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539
ARG WASM_BINDGEN_SHA256=97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240

ENV DEBIAN_FRONTEND=noninteractive \
    BUNDLE_ROOT=/opt/jazz-ci/toolchains/v1 \
    RUSTUP_HOME=/opt/jazz-ci/toolchains/v1/rustup \
    RUSTUP_TOOLCHAIN=1.93.1-x86_64-unknown-linux-gnu \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PATH=/opt/jazz-ci/toolchains/v1/bin:/opt/jazz-ci/toolchains/v1/node/bin:/opt/jazz-ci/toolchains/v1/rustup/toolchains/1.93.1-x86_64-unknown-linux-gnu/bin:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential ca-certificates clang cmake curl git jq libclang-dev libssl-dev \
      lsof ninja-build pkg-config procps python3 rsync sudo tar unzip util-linux xz-utils zip zstd \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p "${BUNDLE_ROOT}/bin" "${BUNDLE_ROOT}/node" \
    && curl --fail --location --silent --show-error \
      --output /tmp/node.tar.xz \
      "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
    && echo "${NODE_SHA256}  /tmp/node.tar.xz" | sha256sum --check --strict \
    && tar -xJ --strip-components=1 -C "${BUNDLE_ROOT}/node" -f /tmp/node.tar.xz \
    && npm install --global --prefix "${BUNDLE_ROOT}/node" "pnpm@${PNPM_VERSION}" \
    && curl --fail --location --silent --show-error --output /tmp/rustup-init \
      https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init \
    && echo "${RUSTUP_INIT_SHA256}  /tmp/rustup-init" | sha256sum --check --strict \
    && chmod +x /tmp/rustup-init \
    && /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain "${RUST_VERSION}" \
    && /root/.cargo/bin/rustup component add clippy rustfmt \
    && /root/.cargo/bin/rustup target add wasm32-unknown-unknown \
    && install -m 0755 /root/.cargo/bin/rustup "${BUNDLE_ROOT}/bin/rustup" \
    && rm -rf /root/.cargo /tmp/node.tar.xz /tmp/rustup-init

RUN set -eux; \
    install_release() { \
      url="$1"; sha256="$2"; binary="$3"; \
      work="$(mktemp -d)"; \
      curl --fail --location --silent --show-error --output "${work}/release.tar.gz" "${url}"; \
      echo "${sha256}  ${work}/release.tar.gz" | sha256sum --check --strict; \
      tar -xzf "${work}/release.tar.gz" -C "${work}"; \
      found="$(find "${work}" -type f -name "${binary}" -perm -u+x | head -n 1)"; \
      test -n "${found}"; \
      install -m 0755 "${found}" "${BUNDLE_ROOT}/bin/${binary}"; \
      rm -rf "${work}"; \
    }; \
    install_release \
      "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
      "${SCCACHE_SHA256}" \
      sccache; \
    install_release \
      "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${CARGO_NEXTEST_VERSION}/cargo-nextest-${CARGO_NEXTEST_VERSION}-x86_64-unknown-linux-gnu.tar.gz" \
      "${CARGO_NEXTEST_SHA256}" \
      cargo-nextest; \
    install_release \
      "https://github.com/rustwasm/wasm-pack/releases/download/v${WASM_PACK_VERSION}/wasm-pack-v${WASM_PACK_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
      "${WASM_PACK_SHA256}" \
      wasm-pack; \
    install_release \
      "https://github.com/wasm-bindgen/wasm-bindgen/releases/download/${WASM_BINDGEN_VERSION}/wasm-bindgen-${WASM_BINDGEN_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
      "${WASM_BINDGEN_SHA256}" \
      wasm-bindgen

RUN pnpm dlx "playwright@${PLAYWRIGHT_VERSION}" install --with-deps chromium \
    && rm -rf /root/.cache /tmp/*

RUN printf '%s\n' \
      '{' \
      '  "bundle": "blacksmith-v1",' \
      '  "architecture": "x86_64-unknown-linux-gnu",' \
      "  \"node\": \"${NODE_VERSION}\"," \
      "  \"pnpm\": \"${PNPM_VERSION}\"," \
      "  \"rust\": \"${RUST_VERSION}\"," \
      '  "rustComponents": ["clippy", "rustfmt"],' \
      '  "rustTargets": ["wasm32-unknown-unknown"],' \
      "  \"sccache\": \"${SCCACHE_VERSION}\"," \
      "  \"cargoNextest\": \"${CARGO_NEXTEST_VERSION}\"," \
      "  \"wasmPack\": \"${WASM_PACK_VERSION}\"," \
      "  \"wasmBindgenCli\": \"${WASM_BINDGEN_VERSION}\"," \
      "  \"playwright\": \"${PLAYWRIGHT_VERSION}\"," \
      '  "chromium": "preinstalled",' \
      '  "sha256": {' \
      "    \"node\": \"${NODE_SHA256}\"," \
      "    \"rustupInit\": \"${RUSTUP_INIT_SHA256}\"," \
      "    \"sccache\": \"${SCCACHE_SHA256}\"," \
      "    \"cargoNextest\": \"${CARGO_NEXTEST_SHA256}\"," \
      "    \"wasmPack\": \"${WASM_PACK_SHA256}\"," \
      "    \"wasmBindgenCli\": \"${WASM_BINDGEN_SHA256}\"" \
      '  }' \
      '}' > "${BUNDLE_ROOT}/manifest.json" \
    && chmod -R a-w "${BUNDLE_ROOT}" "${PLAYWRIGHT_BROWSERS_PATH}"

import assert from "node:assert/strict";
import test from "node:test";

import { validateActiveEnvironment, validateManifest } from "../../ci/validate-tool-bundle.mjs";

const manifest = {
  bundle: "jazz-ci-v1",
  architecture: "x86_64",
  node: "24.13.0",
  pnpm: "10.14.0",
  rust: "1.93.1",
  rustComponents: ["clippy", "rustfmt"],
  rustTargets: ["wasm32-unknown-unknown"],
  sccache: "0.15.0",
  cargoNextest: "0.9.143",
  wasmPack: "0.13.1",
  wasmBindgenCli: "0.2.117",
};

const environment = {
  RUSTUP_HOME: "/opt/jazz-ci/toolchains/v1/rustup",
  RUSTUP_TOOLCHAIN: "1.93.1-x86_64-unknown-linux-gnu",
  SCCACHE_SERVER_PORT: "4226",
  SCCACHE_DIR: "/var/cache/jazz-sccache",
  SCCACHE_CACHE_SIZE: "100G",
  SCCACHE_IDLE_TIMEOUT: "0",
};

const versions = {
  node: "v24.13.0",
  pnpm: "10.14.0",
  rustc: "rustc 1.93.1",
  sccache: "sccache 0.15.0",
  "cargo-nextest": "cargo-nextest 0.9.143",
  "wasm-pack": "wasm-pack 0.13.1",
  "wasm-bindgen": "wasm-bindgen 0.2.117",
};

test("provisioned CI bundle accepts the exact immutable tool and daemon contract", () => {
  validateManifest(manifest);
  validateManifest({
    ...manifest,
    rustComponents: [...manifest.rustComponents, "llvm-tools"],
    rustTargets: [...manifest.rustTargets, "x86_64-unknown-linux-gnu"],
  });
  validateActiveEnvironment(environment, (command) => versions[command]);
});

test("provisioned CI bundle fails closed on version or daemon drift", () => {
  assert.throws(() => validateManifest({ ...manifest, node: "24.13.1" }), /node must be/);
  assert.throws(() => validateManifest({ ...manifest, rustTargets: [] }), /rustTargets is missing/);
  assert.throws(() => validateManifest({ ...manifest, rust: "1.94.0" }), /rust must be/);
  assert.throws(
    () =>
      validateActiveEnvironment(
        { ...environment, SCCACHE_SERVER_PORT: "4227" },
        (command) => versions[command],
      ),
    /SCCACHE_SERVER_PORT/,
  );
  assert.throws(
    () =>
      validateActiveEnvironment(
        { ...environment, SCCACHE_SERVER_UDS: "/tmp/rogue.sock" },
        (command) => versions[command],
      ),
    /SCCACHE_SERVER_UDS/,
  );
  assert.throws(
    () =>
      validateActiveEnvironment(environment, (command) =>
        command === "sccache" ? "sccache 0.14.0" : versions[command],
      ),
    /sccache must report 0.15.0/,
  );
});

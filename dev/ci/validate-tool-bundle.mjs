#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

export const BUNDLE_ROOT = "/opt/jazz-ci/toolchains/v1";
export const MANIFEST_PATH = `${BUNDLE_ROOT}/manifest.json`;

const expected = {
  // `.nvmrc` uses the abbreviated `24.13`; the provisioned manifest records
  // the fully resolved semantic version.
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

function fail(message) {
  throw new Error(`invalid Jazz CI tool bundle: ${message}`);
}

function requiredSubset(actual, required, field) {
  if (!Array.isArray(actual)) fail(`${field} must be an array, got ${JSON.stringify(actual)}`);
  const missing = required.filter((entry) => !actual.includes(entry));
  if (missing.length > 0)
    fail(`${field} is missing ${JSON.stringify(missing)}, got ${JSON.stringify(actual)}`);
}

export function validateManifest(manifest) {
  if (typeof manifest.bundle !== "string" || manifest.bundle.length === 0)
    fail("bundle must be a non-empty string");
  if (typeof manifest.architecture !== "string" || manifest.architecture.length === 0) {
    fail("architecture must be a non-empty string");
  }
  for (const [field, wanted] of Object.entries(expected)) {
    if (Array.isArray(wanted)) {
      // The host may provision additional targets/components for other jobs.
      // Require our inputs without rejecting a useful immutable superset.
      requiredSubset(manifest[field], wanted, field);
    } else if (manifest[field] !== wanted) {
      fail(`${field} must be ${JSON.stringify(wanted)}, got ${JSON.stringify(manifest[field])}`);
    }
  }
}

function commandVersion(command, args = ["--version"]) {
  return execFileSync(command, args, { encoding: "utf8" }).trim();
}

export function validateActiveEnvironment(env = process.env, readVersion = commandVersion) {
  if (env.RUSTUP_HOME !== `${BUNDLE_ROOT}/rustup`)
    fail(`unexpected RUSTUP_HOME ${env.RUSTUP_HOME}`);
  if (env.RUSTUP_TOOLCHAIN !== "1.93.1-x86_64-unknown-linux-gnu") {
    fail(`unexpected RUSTUP_TOOLCHAIN ${env.RUSTUP_TOOLCHAIN}`);
  }
  if (env.SCCACHE_SERVER_PORT !== "4226")
    fail(`unexpected SCCACHE_SERVER_PORT ${env.SCCACHE_SERVER_PORT}`);
  if (env.SCCACHE_DIR !== "/var/cache/jazz-sccache")
    fail(`unexpected SCCACHE_DIR ${env.SCCACHE_DIR}`);
  if (env.SCCACHE_CACHE_SIZE !== "100G")
    fail(`unexpected SCCACHE_CACHE_SIZE ${env.SCCACHE_CACHE_SIZE}`);
  if (env.SCCACHE_IDLE_TIMEOUT !== "0")
    fail(`unexpected SCCACHE_IDLE_TIMEOUT ${env.SCCACHE_IDLE_TIMEOUT}`);
  if (env.SCCACHE_SERVER_UDS) fail("SCCACHE_SERVER_UDS must be unset for the managed TCP daemon");

  const commands = [
    ["node", expected.node],
    ["pnpm", expected.pnpm],
    ["rustc", expected.rust],
    ["sccache", expected.sccache],
    ["cargo-nextest", expected.cargoNextest],
    ["wasm-pack", expected.wasmPack],
    ["wasm-bindgen", expected.wasmBindgenCli],
  ];
  for (const [command, wanted] of commands) {
    const actual = readVersion(command);
    if (!actual.includes(wanted)) fail(`${command} must report ${wanted}, got ${actual}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const manifest = JSON.parse(readFileSync(MANIFEST_PATH, "utf8"));
  validateManifest(manifest);
  validateActiveEnvironment();
  process.stdout.write(`${JSON.stringify(manifest)}\n`);
}

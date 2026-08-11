#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { writeManifest } from "./provenance.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const [kind, profile = "release"] = process.argv.slice(2);
const commands = {
  wasm: {
    fast: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--dev"]],
    release: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--release"]],
    profiling: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--profiling"]],
  },
  napi: {
    debug: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform"]],
    release: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--release"]],
  },
};
const selected = commands[kind]?.[profile];
if (!selected) throw new Error("usage: build.mjs <wasm fast|release|profiling | napi debug|release>");
const [command, args] = selected;
const result = spawnSync(command, args, { cwd: root, stdio: "inherit", shell: process.platform === "win32" });
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
writeManifest(root, kind, profile);

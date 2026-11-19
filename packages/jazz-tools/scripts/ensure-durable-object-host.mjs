#!/usr/bin/env node
// The Durable Object tests run jazz-wasm's Durable Object build: build it
// when it isn't there (a fresh checkout, CI). An existing build is used as
// is; after changing the Rust, run `pnpm --filter jazz-wasm build:durable-object`.
//
// `--wasm-opt-level O1` builds it with a cheaper final wasm-opt pass (same
// Rust and LTO); merge-group CI uses it (see crates/jazz-wasm/durable-object/
// build.mjs). The default is the -Oz build that ships.
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const args = process.argv.slice(2);
if (!(args.length === 0 || (args.length === 2 && args[0] === "--wasm-opt-level"))) {
  console.error("usage: ensure-durable-object-host.mjs [--wasm-opt-level LEVEL]");
  process.exit(2);
}

const jazzWasm = dirname(createRequire(import.meta.url).resolve("jazz-wasm/package.json"));
if (!existsSync(join(jazzWasm, "durable-object/pkg/jazz_wasm_do_bg.wasm"))) {
  console.log("jazz-tools: building jazz-wasm's Durable Object host for the Durable Object tests");
  execFileSync("pnpm", ["--filter", "jazz-wasm", "build:durable-object", ...args], {
    stdio: "inherit",
  });
}

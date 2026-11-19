#!/usr/bin/env node
// The Durable Object tests run jazz-wasm's Durable Object build: build it
// when it isn't there (a fresh checkout, CI). An existing build is used as
// is; after changing the Rust, run `pnpm --filter jazz-wasm build:durable-object`.
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const jazzWasm = dirname(createRequire(import.meta.url).resolve("jazz-wasm/package.json"));
if (!existsSync(join(jazzWasm, "durable-object/pkg/jazz_wasm_do_bg.wasm"))) {
  console.log("jazz-tools: building jazz-wasm's Durable Object host for the Durable Object tests");
  execFileSync("pnpm", ["--filter", "jazz-wasm", "build:durable-object"], { stdio: "inherit" });
}

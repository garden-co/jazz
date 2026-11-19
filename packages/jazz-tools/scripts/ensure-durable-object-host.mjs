#!/usr/bin/env node
// The Durable Object tests run jazz-wasm's two Worker builds: the Durable
// Object core (`durable-object/pkg`) and the backend build a backend's
// `createJazz` runs on (`backend/pkg`). Build each when it isn't there (a
// fresh checkout, CI). An existing build is used as is; after changing the
// Rust, run `pnpm --filter jazz-wasm build:durable-object` and
// `pnpm --filter jazz-wasm build:backend`.
//
// `--wasm-opt-level O1` builds them with a cheaper final wasm-opt pass (same
// Rust and LTO); merge-group CI uses it (see crates/jazz-wasm/size-build.mjs).
// The default is the -Oz build that ships.
//
// From a clean checkout, a fresh build first looks in the native build cache
// (dev/artifacts/native-build-cache.mjs) for the exact build of these inputs:
// the wasm crate's native input fingerprint (its whole dependency closure,
// including jazz-server and the build scripts), the build arguments (so the
// build and the wasm-opt level), the tools and the build environment. The
// build itself is the same `build:durable-object` or `build:backend`; CI
// restores those entries from the main branch's warm-native-builds job.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeBuildCache } from "../../../dev/artifacts/native-build-cache.mjs";

const args = process.argv.slice(2);
if (!(args.length === 0 || (args.length === 2 && args[0] === "--wasm-opt-level"))) {
  console.error("usage: ensure-durable-object-host.mjs [--wasm-opt-level LEVEL]");
  process.exit(2);
}

const root = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const jazzWasm = dirname(createRequire(import.meta.url).resolve("jazz-wasm/package.json"));

const builds = [
  {
    profile: "durable-object",
    what: "Durable Object host",
    pkg: join(jazzWasm, "durable-object/pkg"),
    wasm: "jazz_wasm_do_bg.wasm",
  },
  {
    profile: "backend",
    what: "backend build",
    pkg: join(jazzWasm, "backend/pkg"),
    wasm: "jazz_wasm_backend_bg.wasm",
  },
];

for (const { profile, what, pkg, wasm } of builds) {
  if (existsSync(join(pkg, wasm))) continue;
  const command = ["pnpm", "--filter", "jazz-wasm", `build:${profile}`, ...args];
  const cache = nativeBuildCache(
    root,
    { kind: "wasm", profile, target: undefined, args: command },
    { requireLease: false },
  );
  rmSync(pkg, { recursive: true, force: true });
  mkdirSync(pkg, { recursive: true });
  if (!cache?.restore(pkg)) {
    rmSync(pkg, { recursive: true, force: true });
    console.log(`jazz-tools: building jazz-wasm's ${what} for the Durable Object tests`);
    execFileSync(command[0], command.slice(1), { stdio: "inherit" });
    if (!existsSync(join(pkg, wasm)) || !readdirSync(pkg).length)
      throw new Error(`jazz-tools: build:${profile} produced no ${what}`);
    cache?.capture(pkg).commit();
  }
}

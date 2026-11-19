// `build:durable-object`: the WASM a Cloudflare Durable Object hosts, built
// for size. A Worker's compressed size is capped (3 MB on the Workers Free
// plan), and the core is the bulk of it, so this build trades some speed for
// size, unlike the speed-tuned browser and Node builds:
// - Rust: opt-level "z", fat LTO, one codegen unit, panic = abort (the host
//   reinstantiates after a trap; see reinstantiable.mjs);
// - wasm-opt -Oz, without the name section (wasm-pack's own wasm-opt step is
//   skipped: its flags are the browser build's).
// Measured on the 2026-11-18 core: 4.07 MB gzip as the browser build's
// settings make it, 2.46 MB this way.
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const out = join(root, "durable-object/pkg");
const env = {
  ...process.env,
  CARGO_PROFILE_RELEASE_OPT_LEVEL: "z",
  CARGO_PROFILE_RELEASE_LTO: "fat",
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS: "1",
  CARGO_PROFILE_RELEASE_PANIC: "abort",
};
const run = (cmd, args) => execFileSync(cmd, args, { cwd: root, env, stdio: "inherit" });

run("wasm-pack", [
  "build",
  ".",
  "--target",
  "web",
  "--release",
  "--no-opt",
  "--out-dir",
  out,
  "--out-name",
  "jazz_wasm_do",
  "--no-pack",
  "--",
  "--features",
  "durable-object",
]);

/**
 * The binaryen the size numbers were measured with, and the one wasm-pack
 * 0.13.1 downloads for the browser build: another version optimises
 * differently, so it is refused rather than used.
 */
const WASM_OPT_VERSION = "version 117";

/** A pinned wasm-opt: on PATH, or the copy wasm-pack downloaded. */
function wasmOpt() {
  const cache = join(homedir(), ".cache/.wasm-pack");
  const candidates = [
    "wasm-opt",
    ...(existsSync(cache) ? readdirSync(cache) : [])
      .filter((dir) => dir.startsWith("wasm-opt"))
      .map((dir) => join(cache, dir, "bin/wasm-opt")),
  ];
  const seen = [];
  for (const bin of candidates) {
    let version;
    try {
      version = execFileSync(bin, ["--version"], { encoding: "utf8" }).trim();
    } catch {
      continue;
    }
    seen.push(`${bin}: ${version}`);
    if (version.includes(WASM_OPT_VERSION)) {
      console.log(`build:durable-object: ${bin} (${version})`);
      return bin;
    }
  }
  throw new Error(
    `build:durable-object needs wasm-opt ${WASM_OPT_VERSION} (binaryen), on PATH or as wasm-pack's copy; found ${seen.join("; ") || "none"}`,
  );
}

const wasm = join(out, "jazz_wasm_do_bg.wasm");
run(wasmOpt(), [
  "-Oz",
  "--strip-debug",
  "--strip-producers",
  "--low-memory-unused",
  "--all-features",
  wasm,
  "-o",
  wasm,
]);
run(process.execPath, [
  join(root, "durable-object/reinstantiable.mjs"),
  join(out, "jazz_wasm_do.js"),
]);
// wasm-pack's `.gitignore` of `*` would keep the build out of the tarball.
rmSync(join(out, ".gitignore"), { force: true });

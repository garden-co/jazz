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
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync } from "node:fs";
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
 * differently, so it is never used.
 */
const BINARYEN = "version_117";
const WASM_OPT_VERSION = "version 117";

/** binaryen's release archives of BINARYEN, by `${process.platform}-${process.arch}`, with their SHA-256. */
const RELEASES = {
  "linux-x64": ["x86_64-linux", "3dc677006555b355ea2da5e82602065a161d5e83eaefd3f759afa00b96e83212"],
  "linux-arm64": [
    "aarch64-linux",
    "ad560204426015a815faa45693c83bef7d58677d38a39422c272a30ba4b6da2a",
  ],
  "darwin-x64": [
    "x86_64-macos",
    "c0be7b448888f2c851a853ca19299b09d42cac8c3090c716d14cc8ec3c0ac849",
  ],
  "darwin-arm64": [
    "arm64-macos",
    "f2d962ff294b38ea3cfbbae8f6c728089d9375a57bac9a1880eb86779d6d3a84",
  ],
};

function versionOf(bin) {
  try {
    return execFileSync(bin, ["--version"], { encoding: "utf8" }).trim();
  } catch {
    return undefined;
  }
}

/**
 * A pinned wasm-opt: on PATH, else wasm-pack's copy, else binaryen's own
 * release archive, downloaded once into ~/.cache/jazz and checked against
 * its SHA-256.
 */
function wasmOpt() {
  const wasmPackCache = join(homedir(), ".cache/.wasm-pack");
  const ours = join(homedir(), ".cache/jazz", `binaryen-${BINARYEN}`);
  const candidates = [
    "wasm-opt",
    ...(existsSync(wasmPackCache) ? readdirSync(wasmPackCache) : [])
      .filter((dir) => dir.startsWith("wasm-opt"))
      .map((dir) => join(wasmPackCache, dir, "bin/wasm-opt")),
    join(ours, "bin/wasm-opt"),
  ];
  const seen = [];
  for (const bin of candidates) {
    const version = versionOf(bin);
    if (version === undefined) continue;
    seen.push(`${bin}: ${version}`);
    if (version.includes(WASM_OPT_VERSION)) {
      console.log(`build:durable-object: ${bin} (${version})`);
      return bin;
    }
  }
  const release = RELEASES[`${process.platform}-${process.arch}`];
  if (!release) {
    throw new Error(
      `build:durable-object needs wasm-opt ${WASM_OPT_VERSION} (binaryen); none on PATH or from wasm-pack (${seen.join("; ") || "none found"}), and no binaryen release for ${process.platform}-${process.arch} to download`,
    );
  }
  const [name, sha256] = release;
  const archive = `binaryen-${BINARYEN}-${name}.tar.gz`;
  const url = `https://github.com/WebAssembly/binaryen/releases/download/${BINARYEN}/${archive}`;
  console.log(`build:durable-object: downloading ${url}`);
  mkdirSync(ours, { recursive: true });
  const file = join(ours, archive);
  execFileSync("curl", ["-sSfL", "--retry", "3", "-o", file, url], { stdio: "inherit" });
  const actual = createHash("sha256").update(readFileSync(file)).digest("hex");
  if (actual !== sha256) {
    rmSync(file, { force: true });
    throw new Error(`build:durable-object: ${archive} has SHA-256 ${actual}, expected ${sha256}`);
  }
  execFileSync("tar", ["-xzf", file, "-C", ours, "--strip-components", "1"], { stdio: "inherit" });
  rmSync(file, { force: true });
  const bin = join(ours, "bin/wasm-opt");
  const version = versionOf(bin);
  if (!version?.includes(WASM_OPT_VERSION)) {
    throw new Error(`build:durable-object: the downloaded ${bin} reports ${version ?? "nothing"}`);
  }
  console.log(`build:durable-object: ${bin} (${version})`);
  return bin;
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

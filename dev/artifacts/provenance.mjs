#!/usr/bin/env node
/**
 * Content-addressed provenance for the generated bindings.  This intentionally
 * uses only Node and git: both are already required by the repository, unlike
 * platform-specific stat/hash utilities.
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const sha256 = (value) => createHash("sha256").update(value).digest("hex");
const run = (root, command, args) => {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8" });
  return result.status === 0 ? result.stdout.trim() : "unavailable";
};

const toolEnvName = (name) => `JAZZ_ARTIFACT_TOOL_${name.toUpperCase().replaceAll("-", "_")}`;
function toolVersion(root, name, args = ["--version"]) {
  const injected = process.env[toolEnvName(name)];
  if (injected) return injected;
  const command = name === "napi" ? "pnpm" : name;
  const commandArgs = name === "napi" ? ["--dir", "crates/jazz-napi", "exec", "napi", ...args] : args;
  const result = spawnSync(command, commandArgs, { cwd: root, encoding: "utf8", shell: process.platform === "win32" });
  if (result.status === 0) return result.stdout.trim() || result.stderr.trim();
  if (name === "wasm-pack") return "unavailable: install wasm-pack (run pnpm ensure:rust-toolchain), then rebuild via pnpm --filter jazz-wasm build";
  return `unavailable: ${name} is not on PATH`;
}

function wasmPackToolVersion(root, name) {
  const direct = toolVersion(root, name);
  if (!direct.startsWith("unavailable:")) return direct;
  if (process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE === "1") return `unavailable: ${name} is supplied by wasm-pack; rebuild via pnpm --filter jazz-wasm build`;
  const caches = [process.env.XDG_CACHE_HOME, join(homedir(), ".cache"), join(homedir(), "Library", "Caches")].filter(Boolean);
  const candidates = [];
  for (const cache of caches) {
    const wasmPackCache = join(cache, ".wasm-pack");
    if (!existsSync(wasmPackCache)) continue;
    for (const entry of readdirSync(wasmPackCache)) {
      if (!entry.startsWith(`${name}-`)) continue;
      const executable = name === "wasm-opt" ? join(wasmPackCache, entry, "bin", name) : join(wasmPackCache, entry, name);
      if (existsSync(executable)) candidates.push(executable);
    }
  }
  candidates.sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs);
  if (!candidates.length) return `unavailable: ${name} is supplied by wasm-pack; rebuild via pnpm --filter jazz-wasm build`;
  return toolVersion(root, candidates[0]);
}

function files(root, paths) {
  const found = [];
  const visit = (path) => {
    if (!existsSync(path)) return;
    const stat = statSync(path);
    if (stat.isDirectory()) for (const name of readdirSync(path).sort()) {
      if (["pkg", "target", "node_modules"].includes(name)) continue;
      visit(join(path, name));
    }
    else if (stat.isFile()) {
      const repoPath = relative(root, path);
      if (repoPath.endsWith(".node") || repoPath.endsWith(".jazz-artifact-manifest.json")) return;
      found.push(repoPath);
    }
  };
  for (const path of paths) visit(join(root, path));
  return found.sort();
}

const inputsFor = {
  wasm: ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config", ".cargo/config.toml", "package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml", "turbo.json", "dev/artifacts", "crates/jazz-wasm", "crates/jazz", "crates/groove", "crates/opfs-btree", "crates/wasm-tracing"],
  napi: ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config", ".cargo/config.toml", "package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml", "turbo.json", "dev/artifacts", "crates/jazz-napi", "crates/jazz", "crates/groove", "crates/opfs-btree"],
};

function artifactHashes(root, kind) {
  const paths = kind === "wasm"
    ? [join(root, "crates/jazz-wasm/pkg/jazz_wasm_bg.wasm")]
    : readdirSync(join(root, "crates/jazz-napi"), { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith(".node"))
      .map((entry) => join(root, "crates/jazz-napi", entry.name));
  return paths.filter(existsSync).sort().map((path) => ({ file: basename(path), sha256: sha256(readFileSync(path)) }));
}

export function expectedManifest(root, kind, profile, targetOverride) {
  if (!(kind in inputsFor)) throw new Error(`unknown artifact kind: ${kind}`);
  const trackedInputs = files(root, inputsFor[kind]);
  const inputHash = createHash("sha256");
  for (const path of trackedInputs) {
    inputHash.update(`${path}\0`).update(readFileSync(join(root, path))).update("\0");
  }
  const cargoLock = join(root, "Cargo.lock");
  const toolchain = join(root, "rust-toolchain.toml");
  const injectedGit = process.env.JAZZ_ARTIFACT_GIT_HEAD && process.env.JAZZ_ARTIFACT_GIT_TREE && process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF;
  const tools = {
    rustc: toolVersion(root, "rustc", ["-Vv"]),
    wasmPack: kind === "wasm" ? toolVersion(root, "wasm-pack") : "not-applicable",
    wasmBindgen: kind === "wasm" ? wasmPackToolVersion(root, "wasm-bindgen") : "not-applicable",
    wasmOpt: kind === "wasm" ? wasmPackToolVersion(root, "wasm-opt") : "not-applicable",
    napi: kind === "napi" ? toolVersion(root, "napi") : "not-applicable",
  };
  return {
    schema: 1,
    kind,
    profile,
    git: {
      head: injectedGit ? process.env.JAZZ_ARTIFACT_GIT_HEAD : run(root, "git", ["rev-parse", "HEAD"]),
      tree: injectedGit ? process.env.JAZZ_ARTIFACT_GIT_TREE : run(root, "git", ["rev-parse", "HEAD^{tree}"]),
      // Include staged, unstaged and untracked changes. A dirty build is valid
      // only for that exact dirty checkout, never merely for its HEAD commit.
      dirtyDiff: injectedGit ? process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF : sha256(`${run(root, "git", ["diff", "--binary", "HEAD"])}\n${run(root, "git", ["status", "--porcelain=v1", "--untracked-files=all", "--", ".", ":(exclude)crates/jazz-wasm/pkg/.jazz-artifact-manifest.json", ":(exclude)crates/jazz-napi/.jazz-artifact-manifest.json"])}`),
    },
    cargoLock: existsSync(cargoLock) ? sha256(readFileSync(cargoLock)) : "missing",
    rustToolchain: existsSync(toolchain) ? sha256(readFileSync(toolchain)) : "missing",
    tools,
    toolchainInputs: sha256(JSON.stringify(tools)),
    target: targetOverride ?? (kind === "wasm" ? "wasm32-unknown-unknown" : toolVersion(root, "rustc", ["-vV"]).match(/^host: (.+)$/m)?.[1] ?? "unknown"),
    features: "default",
    packageInputs: inputHash.digest("hex"),
    artifacts: artifactHashes(root, kind),
  };
}

export const manifestPath = (root, kind) => join(root, kind === "wasm" ? "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json" : "crates/jazz-napi/.jazz-artifact-manifest.json");

export function writeManifest(root, kind, profile, targetOverride) {
  const path = manifestPath(root, kind);
  writeFileSync(path, `${JSON.stringify(expectedManifest(root, kind, profile, targetOverride), null, 2)}\n`);
}

export function verifyManifest(root, kind, profile, targetOverride) {
  const path = manifestPath(root, kind);
  if (!existsSync(path)) return `manifest is missing (${path})`;
  let actual;
  try { actual = JSON.parse(readFileSync(path, "utf8")); } catch { return `manifest is invalid (${path})`; }
  const expected = expectedManifest(root, kind, profile, targetOverride);
  for (const key of ["schema", "kind", "profile", "cargoLock", "rustToolchain", "toolchainInputs", "target", "features", "packageInputs", "artifacts"]) {
    if (JSON.stringify(actual[key]) !== JSON.stringify(expected[key])) return `${key} differs (built ${JSON.stringify(actual[key])}, expected ${JSON.stringify(expected[key])})`;
  }
  for (const key of ["rustc", "wasmPack", "wasmBindgen", "wasmOpt", "napi"]) {
    if (actual.tools?.[key] !== expected.tools[key]) return `tools.${key} differs (built ${JSON.stringify(actual.tools?.[key])}, expected ${JSON.stringify(expected.tools[key])})`;
  }
  for (const key of ["head", "tree", "dirtyDiff"]) if (actual.git?.[key] !== expected.git[key]) return `git.${key} differs`;
  return null;
}

export function verifyPublishedNapiManifest(manifest, target, nodePath) {
  if (manifest.kind !== "napi" || manifest.profile !== "release" || manifest.target !== target) return `manifest is for ${manifest.kind}/${manifest.profile}/${manifest.target}, expected napi/release/${target}`;
  if (!existsSync(nodePath)) return `native binding is missing (${nodePath})`;
  const expected = { file: basename(nodePath), sha256: sha256(readFileSync(nodePath)) };
  return manifest.artifacts?.some((artifact) => artifact.file === expected.file && artifact.sha256 === expected.sha256) ? null : `manifest does not match ${expected.file}`;
}

function main(args) {
  const [command, kind, profile] = args;
  const rootFlag = args.indexOf("--root");
  const root = rootFlag === -1 ? here : resolve(args[rootFlag + 1]);
  const targetFlag = args.indexOf("--target");
  const target = targetFlag === -1 ? undefined : args[targetFlag + 1];
  if (!command || !kind || !profile || !["wasm", "napi"].includes(kind)) throw new Error("usage: provenance.mjs <write|verify> <wasm|napi> <profile> [--root path]");
  if (command === "write") { writeManifest(root, kind, profile, target); return; }
  if (command === "verify") {
    const problem = verifyManifest(root, kind, profile, target);
    if (problem) { console.error(`STALE ${kind} ${profile}: ${problem}`); process.exitCode = 1; }
    else console.log(`FRESH ${kind} ${profile}`);
    return;
  }
  throw new Error(`unknown command: ${command}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try { main(process.argv.slice(2)); } catch (error) { console.error(`artifact provenance: ${error.message}`); process.exitCode = 2; }
}

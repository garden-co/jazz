#!/usr/bin/env node
/**
 * Content-addressed provenance for the generated bindings.  This intentionally
 * uses only Node and git: both are already required by the repository, unlike
 * platform-specific stat/hash utilities.
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const sha256 = (value) => createHash("sha256").update(value).digest("hex");
const run = (root, command, args) => {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8" });
  return result.status === 0 ? result.stdout.trim() : "unavailable";
};

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
  wasm: ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "dev/artifacts", "crates/jazz-wasm", "crates/jazz", "crates/groove", "crates/opfs-btree", "crates/wasm-tracing"],
  napi: ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "dev/artifacts", "crates/jazz-napi", "crates/jazz", "crates/groove", "crates/opfs-btree"],
};

export function expectedManifest(root, kind, profile) {
  if (!(kind in inputsFor)) throw new Error(`unknown artifact kind: ${kind}`);
  const trackedInputs = files(root, inputsFor[kind]);
  const inputHash = createHash("sha256");
  for (const path of trackedInputs) {
    inputHash.update(`${path}\0`).update(readFileSync(join(root, path))).update("\0");
  }
  const cargoLock = join(root, "Cargo.lock");
  const toolchain = join(root, "rust-toolchain.toml");
  const injectedGit = process.env.JAZZ_ARTIFACT_GIT_HEAD && process.env.JAZZ_ARTIFACT_GIT_TREE && process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF;
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
    rustc: run(root, "rustc", ["-Vv"]),
    target: kind === "wasm" ? "wasm32-unknown-unknown" : run(root, "rustc", ["-vV"]).match(/^host: (.+)$/m)?.[1] ?? "unknown",
    features: "default",
    packageInputs: inputHash.digest("hex"),
  };
}

export const manifestPath = (root, kind) => join(root, kind === "wasm" ? "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json" : "crates/jazz-napi/.jazz-artifact-manifest.json");

export function writeManifest(root, kind, profile) {
  const path = manifestPath(root, kind);
  writeFileSync(path, `${JSON.stringify(expectedManifest(root, kind, profile), null, 2)}\n`);
}

export function verifyManifest(root, kind, profile) {
  const path = manifestPath(root, kind);
  if (!existsSync(path)) return `manifest is missing (${path})`;
  let actual;
  try { actual = JSON.parse(readFileSync(path, "utf8")); } catch { return `manifest is invalid (${path})`; }
  const expected = expectedManifest(root, kind, profile);
  for (const key of ["schema", "kind", "profile", "cargoLock", "rustToolchain", "rustc", "target", "features", "packageInputs"]) {
    if (actual[key] !== expected[key]) return `${key} differs (built ${JSON.stringify(actual[key])}, expected ${JSON.stringify(expected[key])})`;
  }
  for (const key of ["head", "tree", "dirtyDiff"]) if (actual.git?.[key] !== expected.git[key]) return `git.${key} differs`;
  return null;
}

function main(args) {
  const [command, kind, profile] = args;
  const rootFlag = args.indexOf("--root");
  const root = rootFlag === -1 ? here : resolve(args[rootFlag + 1]);
  if (!command || !kind || !profile || !["wasm", "napi"].includes(kind)) throw new Error("usage: provenance.mjs <write|verify> <wasm|napi> <profile> [--root path]");
  if (command === "write") { writeManifest(root, kind, profile); return; }
  if (command === "verify") {
    const problem = verifyManifest(root, kind, profile);
    if (problem) { console.error(`STALE ${kind} ${profile}: ${problem}`); process.exitCode = 1; }
    else console.log(`FRESH ${kind} ${profile}`);
    return;
  }
  throw new Error(`unknown command: ${command}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try { main(process.argv.slice(2)); } catch (error) { console.error(`artifact provenance: ${error.message}`); process.exitCode = 2; }
}

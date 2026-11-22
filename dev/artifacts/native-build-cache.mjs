/**
 * A content-addressed cache of raw native compiler output (the jazz-napi
 * binding or the wasm-pack package) for the correctness producer.
 *
 * It replaces only the cargo / wasm-pack invocation in dev/artifacts/build.mjs.
 * A restored entry lands in the same fresh stage directory a build would have
 * filled, and everything after it runs unchanged: stage validation (the NAPI
 * binding must load and report the expected fingerprint), the sealed manifest
 * written for this checkout, atomic publication, manifest verification, and
 * the producer's snapshot and receipt.
 *
 * The key is everything that determines the compiler output: the artifact's
 * native input fingerprint (every tracked Rust input of the crate's dependency
 * closure, the build wrappers, the declared surface and features), its
 * profile and target, the exact tool versions, the build arguments, and the
 * compiler-affecting environment. Entries are used only:
 * - by builds run under the correctness producer's artifact lease (package and
 *   release builds never read it), and
 * - from a clean checkout, where the tracked inputs are the whole input set
 *   (an untracked source file is not in the fingerprint).
 * Every file is checked against its recorded hash before it is used.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { join, relative } from "node:path";
import { artifactFeatures, artifactTools, nativeArtifactFingerprint } from "./provenance.mjs";

export const NATIVE_BUILD_CACHE_DIR = "node_modules/.cache/jazz-native-builds";
const KEEP_ENTRIES = 6;
const sha256 = (value) => createHash("sha256").update(value).digest("hex");

// Producer-owned build output that never makes a checkout dirty (the same
// paths provenance.mjs excludes from a manifest's dirty diff).
const generatedPathspecs = [
  ":(exclude)crates/jazz-wasm/pkg/**",
  ":(exclude)crates/jazz-wasm/.pkg-stage-*",
  ":(exclude)crates/jazz-wasm/.pkg-backup-*",
  ":(exclude)crates/jazz-wasm/.pkg-transaction.json*",
  ":(exclude)crates/jazz-wasm/.jazz-correctness-test-artifacts.json",
  ":(exclude)crates/jazz-napi/.jazz-artifact-manifest.json",
  ":(exclude)crates/jazz-napi/native-binding.pointer.cjs",
  ":(exclude)crates/jazz-napi/correctness-native-binding.pointer.cjs",
  ":(exclude)crates/jazz-napi/native-binding.d.ts",
  ":(exclude)crates/jazz-napi/native-artifact-fingerprint.cjs",
  ":(exclude)crates/jazz-napi/native-loader.cjs",
  ":(exclude)crates/jazz-napi/.native-artifacts/**",
  ":(exclude)packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts",
  ":(exclude)packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts",
];

/** Environment that changes what rustc, cargo, cc or wasm-pack produce. */
export function compilerEnvironment(env = process.env) {
  return Object.fromEntries(
    Object.entries(env)
      .filter(
        ([name]) =>
          /^(RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|RUSTC|RUSTC_BOOTSTRAP|CARGO_PROFILE_.+|CARGO_BUILD_.+|CARGO_TARGET_.+|CC|CXX|AR|LD|CFLAGS|CXXFLAGS|LDFLAGS|CPPFLAGS|(CC|CXX|AR|CFLAGS|CXXFLAGS)_.+|WASM_.+|JAZZ_NAPI_RELEASE|JAZZ_RN_TEST_BRIDGE|JAZZ_NAPI_BUILD_.+)$/.test(
            name,
          ) &&
          // These change where output goes or how many jobs build it, not
          // the output.
          !/^(CARGO_BUILD_JOBS|CARGO_TARGET_DIR|CARGO_BUILD_TARGET_DIR)$/.test(name),
      )
      .sort(([a], [b]) => a.localeCompare(b)),
  );
}

export function isCleanCheckout(root, run = spawnSync) {
  const status = run(
    "git",
    ["status", "--porcelain=v1", "--untracked-files=all", "--", ".", ...generatedPathspecs],
    { cwd: root, encoding: "utf8" },
  );
  return status.status === 0 && status.stdout === "";
}

export function nativeBuildCacheKey(
  root,
  { kind, profile, target, args, env = process.env, tools = artifactTools(root, kind) },
) {
  return sha256(
    JSON.stringify({
      schema: 1,
      kind,
      profile,
      target: target ?? null,
      features: artifactFeatures(kind),
      fingerprint: nativeArtifactFingerprint(root, kind, profile, target),
      tools,
      args,
      env: compilerEnvironment(env),
    }),
  );
}

function listFiles(directory) {
  const files = [];
  const walk = (current) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = join(current, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`native build cache: symlink ${entry.name}`);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) files.push(relative(directory, path));
      else throw new Error(`native build cache: unexpected file type ${entry.name}`);
    }
  };
  walk(directory);
  return files.sort();
}

function receipt(directory) {
  return listFiles(directory).map((file) => ({
    file,
    sha256: sha256(readFileSync(join(directory, file))),
  }));
}

/**
 * Copy a cached entry into `destination` (an existing, empty stage). Returns
 * false, leaving `destination` empty, when there is no intact entry.
 */
export function restoreNativeBuild(cacheRoot, key, destination) {
  const entry = join(cacheRoot, key);
  const receiptPath = join(cacheRoot, `${key}.json`);
  if (!existsSync(entry) || !existsSync(receiptPath)) return false;
  try {
    if (!lstatSync(entry).isDirectory()) throw new Error("entry is not a directory");
    const recorded = JSON.parse(readFileSync(receiptPath, "utf8"));
    if (recorded.key !== key) throw new Error("receipt names another key");
    if (JSON.stringify(receipt(entry)) !== JSON.stringify(recorded.files))
      throw new Error("entry does not match its receipt");
    if (readdirSync(destination).length) throw new Error("stage is not empty");
    cpSync(entry, destination, { recursive: true });
    if (JSON.stringify(receipt(destination)) !== JSON.stringify(recorded.files))
      throw new Error("copied stage does not match the receipt");
    const now = new Date();
    utimesSync(receiptPath, now, now);
    return true;
  } catch (error) {
    console.warn(`native build cache: ignoring entry ${key.slice(0, 12)} (${error.message})`);
    for (const name of readdirSync(destination))
      rmSync(join(destination, name), { recursive: true, force: true });
    rmSync(entry, { recursive: true, force: true });
    rmSync(receiptPath, { force: true });
    return false;
  }
}

/**
 * Copy raw compiler output aside now; `commit()` records it under `key` once
 * the build that produced it has passed validation and been published, and
 * `discard()` drops it otherwise.
 */
export function captureNativeBuild(cacheRoot, key, source) {
  const staging = join(cacheRoot, `.staging-${key}-${process.pid}`);
  try {
    mkdirSync(cacheRoot, { recursive: true });
    rmSync(staging, { recursive: true, force: true });
    cpSync(source, staging, { recursive: true });
  } catch (error) {
    rmSync(staging, { recursive: true, force: true });
    console.warn(`native build cache: could not capture ${key.slice(0, 12)} (${error.message})`);
    return { commit() {}, discard() {} };
  }
  return {
    commit: () => storeNativeBuild(cacheRoot, key, staging),
    discard: () => rmSync(staging, { recursive: true, force: true }),
  };
}

/** Move `staging` (raw compiler output) into the cache as `key`, keeping the newest entries. */
export function storeNativeBuild(cacheRoot, key, staging) {
  try {
    const files = receipt(staging);
    rmSync(join(cacheRoot, key), { recursive: true, force: true });
    renameSync(staging, join(cacheRoot, key));
    writeFileSync(join(cacheRoot, `${key}.json`), `${JSON.stringify({ key, files })}\n`);
  } catch (error) {
    rmSync(staging, { recursive: true, force: true });
    console.warn(`native build cache: could not store ${key.slice(0, 12)} (${error.message})`);
    return;
  }
  const receipts = readdirSync(cacheRoot)
    .filter((name) => /^[a-f0-9]{64}\.json$/.test(name))
    .map((name) => ({ name, mtime: statSync(join(cacheRoot, name)).mtimeMs }))
    .sort((a, b) => b.mtime - a.mtime);
  for (const { name } of receipts.slice(KEEP_ENTRIES)) {
    rmSync(join(cacheRoot, name), { force: true });
    rmSync(join(cacheRoot, name.slice(0, -".json".length)), { recursive: true, force: true });
  }
}

/**
 * The cache for one build, or undefined when this build must not use it: not
 * under the correctness producer's lease, or from a checkout whose tracked
 * files are not the whole input set.
 */
export function nativeBuildCache(
  root,
  build,
  { env = process.env, clean = isCleanCheckout, requireLease = true } = {},
) {
  if (requireLease && !env.JAZZ_ARTIFACT_BUILD_LEASE) return undefined;
  if (!clean(root)) {
    console.log("native build cache: checkout has uncommitted changes; building without it");
    return undefined;
  }
  const cacheRoot = join(root, NATIVE_BUILD_CACHE_DIR);
  const key = nativeBuildCacheKey(root, { ...build, env });
  const label = `${build.kind} ${build.profile} ${key.slice(0, 12)}`;
  return {
    key,
    restore(destination) {
      const restored = restoreNativeBuild(cacheRoot, key, destination);
      console.log(
        restored
          ? `native build cache: reusing ${label}; validating it as a fresh build`
          : `native build cache: no entry for ${label}; building`,
      );
      return restored;
    },
    capture(source) {
      return captureNativeBuild(cacheRoot, key, source);
    },
    evict() {
      rmSync(join(cacheRoot, key), { recursive: true, force: true });
      rmSync(join(cacheRoot, `${key}.json`), { force: true });
      console.warn(`native build cache: evicted ${label} after it failed validation`);
    },
  };
}

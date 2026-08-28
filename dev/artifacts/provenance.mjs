#!/usr/bin/env node
/**
 * Content-addressed provenance for the generated bindings.  This intentionally
 * uses only Node and git: both are already required by the repository, unlike
 * platform-specific stat/hash utilities.
 */
import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, join, relative, resolve } from "node:path";
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
  const commandArgs =
    name === "napi" ? ["--dir", "crates/jazz-napi", "exec", "napi", ...args] : args;
  const result = spawnSync(command, commandArgs, {
    cwd: root,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (result.status === 0) return result.stdout.trim() || result.stderr.trim();
  if (name === "wasm-pack")
    return "unavailable: install wasm-pack (run pnpm ensure:rust-toolchain), then rebuild via pnpm --filter jazz-wasm build";
  return `unavailable: ${name} is not on PATH`;
}

function wasmPackToolVersion(root, name) {
  const direct = toolVersion(root, name);
  if (!direct.startsWith("unavailable:")) return direct;
  if (process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE === "1")
    return `unavailable: ${name} is supplied by wasm-pack; rebuild via pnpm --filter jazz-wasm build`;
  const caches = [
    process.env.XDG_CACHE_HOME,
    join(homedir(), ".cache"),
    join(homedir(), "Library", "Caches"),
  ].filter(Boolean);
  const candidates = [];
  for (const cache of caches) {
    const wasmPackCache = join(cache, ".wasm-pack");
    if (!existsSync(wasmPackCache)) continue;
    for (const entry of readdirSync(wasmPackCache)) {
      if (!entry.startsWith(`${name}-`)) continue;
      const executable =
        name === "wasm-opt"
          ? join(wasmPackCache, entry, "bin", name)
          : join(wasmPackCache, entry, name);
      if (existsSync(executable)) candidates.push(executable);
    }
  }
  candidates.sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs);
  if (!candidates.length)
    return `unavailable: ${name} is supplied by wasm-pack; rebuild via pnpm --filter jazz-wasm build`;
  return toolVersion(root, candidates[0]);
}

// build.mjs temporarily renames the current target binding while napi-rs
// produces its replacement.  The suffix intentionally does not end in
// `.node`, so the ordinary generated-artifact exclusion below would otherwise
// mistake it for a source input.  Keep this exact to the build wrapper's
// ephemeral filename contract: real NAPI sources and any other generated
// inputs remain provenance inputs.
const isStagedNapiBinding = (repoPath) =>
  /^crates\/jazz-napi\/jazz-napi\.(?:linux-x64-gnu|win32-x64-msvc|darwin-x64|darwin-arm64)\.node\.staged-\d+-\d+$/.test(
    repoPath,
  );

// napi-rs writes the matching target manifest beside its loadable binding.
// It is ignored, sealed after the producer build, and cannot be a producer
// input without making the native fingerprint depend on lane-local output.
const isNapiGeneratedTargetManifest = (repoPath) =>
  /^crates\/jazz-napi\/jazz-napi\.(?:linux-x64-gnu|win32-x64-msvc|darwin-x64|darwin-arm64)\.manifest\.json$/.test(
    repoPath,
  );

const isNapiGeneratedOutput = (repoPath) =>
  repoPath === "crates/jazz-napi/index.js" ||
  repoPath === "crates/jazz-napi/index.d.ts" ||
  repoPath === "crates/jazz-napi/native-binding.pointer.cjs" ||
  repoPath === "crates/jazz-napi/correctness-native-binding.pointer.cjs" ||
  repoPath === "crates/jazz-napi/native-binding.d.ts" ||
  repoPath === "crates/jazz-napi/native-loader.cjs" ||
  repoPath === "crates/jazz-napi/native-artifact-fingerprint.cjs" ||
  repoPath.startsWith("crates/jazz-napi/.napi-stage-") ||
  repoPath.startsWith("crates/jazz-napi/.native-artifacts/");

function files(root, paths) {
  const found = [];
  const visit = (path) => {
    if (!existsSync(path)) return;
    const stat = statSync(path);
    if (stat.isDirectory())
      for (const name of readdirSync(path).sort()) {
        if (["pkg", "target", "node_modules", ".turbo"].includes(name) || name.startsWith(".pkg-"))
          continue;
        const child = join(path, name);
        visit(child);
      }
    else if (stat.isFile()) {
      const repoPath = relative(root, path);
      if (
        repoPath.endsWith(".node") ||
        repoPath.endsWith(".jazz-artifact-manifest.json") ||
        repoPath === "crates/jazz-wasm/.jazz-correctness-test-artifacts.json" ||
        // These tracked files are generated from packageInputs below. Including
        // them would make the artifact fingerprint self-referential.
        isNapiGeneratedOutput(repoPath) ||
        repoPath.endsWith("native-artifact-fingerprint-napi.ts") ||
        repoPath.endsWith("native-artifact-fingerprint-wasm.ts") ||
        isStagedNapiBinding(repoPath) ||
        isNapiGeneratedTargetManifest(repoPath)
      )
        return;
      found.push(repoPath);
    }
  };
  for (const path of paths) visit(join(root, path));
  return found.sort();
}

const sharedInputs = [
  "Cargo.toml",
  "Cargo.lock",
  "rust-toolchain.toml",
  ".cargo/config",
  ".cargo/config.toml",
  "package.json",
  "pnpm-lock.yaml",
  "pnpm-workspace.yaml",
  "turbo.json",
  "dev/artifacts",
];

const artifactRoots = {
  wasm: "crates/jazz-wasm/Cargo.toml",
  napi: "crates/jazz-napi/Cargo.toml",
};

function workspaceDependencyInputs(root, kind) {
  const result = spawnSync("cargo", ["metadata", "--format-version", "1", "--no-deps"], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    throw new Error(
      `artifact provenance: cargo metadata failed for ${kind}: ${result.stderr.trim() || "unknown error"}`,
    );
  }
  let metadata;
  try {
    metadata = JSON.parse(result.stdout);
  } catch (error) {
    throw new Error(
      `artifact provenance: cargo metadata was invalid for ${kind}: ${error.message}`,
    );
  }
  const packages = new Map(
    metadata.packages.map((pkg) => [resolve(dirname(pkg.manifest_path)), pkg]),
  );
  const rootDirectory = resolve(root, dirname(artifactRoots[kind]));
  if (!packages.has(rootDirectory)) {
    throw new Error(`artifact provenance: cargo metadata omitted ${artifactRoots[kind]}`);
  }
  const pending = [rootDirectory];
  const visited = new Set();
  while (pending.length) {
    const directory = pending.pop();
    if (visited.has(directory)) continue;
    const pkg = packages.get(directory);
    if (!pkg) throw new Error(`artifact provenance: unresolved workspace dependency ${directory}`);
    visited.add(directory);
    for (const dependency of pkg.dependencies) {
      if (!dependency.path || dependency.kind === "dev") continue;
      const dependencyDirectory = resolve(dependency.path);
      if (!packages.has(dependencyDirectory)) {
        throw new Error(
          `artifact provenance: cargo metadata omitted path dependency ${dependency.name} at ${dependencyDirectory}`,
        );
      }
      pending.push(dependencyDirectory);
    }
  }
  return [...visited].map((directory) => relative(root, directory)).sort();
}

const inputsFor = {
  wasm: [...sharedInputs],
  napi: [...sharedInputs],
};

function artifactHashes(root, kind, options = {}) {
  const paths =
    kind === "wasm"
      ? // Browser tests import the generated JS and declarations as well as the
        // binary.  Hash the complete generated binding surface: a stale glue
        // file can silently drop a new Rust argument while the `.wasm` itself is
        // perfectly current.
        [
          join(options.wasmPackageDir ?? join(root, "crates/jazz-wasm/pkg"), "jazz_wasm_bg.wasm"),
          join(options.wasmPackageDir ?? join(root, "crates/jazz-wasm/pkg"), "jazz_wasm.js"),
          join(options.wasmPackageDir ?? join(root, "crates/jazz-wasm/pkg"), "jazz_wasm.d.ts"),
          join(
            options.wasmPackageDir ?? join(root, "crates/jazz-wasm/pkg"),
            "jazz_wasm_bg.wasm.d.ts",
          ),
        ]
      : (options.napiBindings ??
        activeNapiBindings(root) ??
        readdirSync(join(root, "crates/jazz-napi"), { withFileTypes: true })
          .filter((entry) => entry.isFile() && entry.name.endsWith(".node"))
          .map((entry) => join(root, "crates/jazz-napi", entry.name)));
  return paths
    .filter(existsSync)
    .sort()
    .map((path) => ({ file: basename(path), sha256: sha256(readFileSync(path)) }));
}

function activeNapiBindings(root) {
  const packageDir = join(root, "crates", "jazz-napi");
  const pointer = join(packageDir, "native-binding.pointer.cjs");
  if (!existsSync(pointer)) return undefined;
  const match = /\.native-artifacts\/(generation-[A-Za-z0-9.-]+)\/index\.js/.exec(
    readFileSync(pointer, "utf8"),
  );
  if (!match) return undefined;
  const generation = join(packageDir, ".native-artifacts", match[1]);
  if (
    !existsSync(generation) ||
    !lstatSync(generation).isDirectory() ||
    lstatSync(generation).isSymbolicLink()
  )
    return undefined;
  return readdirSync(generation, { withFileTypes: true })
    .filter((entry) => {
      const path = join(generation, entry.name);
      return (
        entry.name.endsWith(".node") &&
        lstatSync(path).isFile() &&
        !lstatSync(path).isSymbolicLink()
      );
    })
    .map((entry) => join(generation, entry.name));
}

function packageInputsFingerprint(root, kind) {
  if (!(kind in inputsFor)) throw new Error(`unknown artifact kind: ${kind}`);
  const trackedInputs = files(root, [...inputsFor[kind], ...workspaceDependencyInputs(root, kind)]);
  const inputHash = createHash("sha256");
  for (const path of trackedInputs) {
    inputHash
      .update(`${path}\0`)
      .update(readFileSync(join(root, path)))
      .update("\0");
  }
  return inputHash.digest("hex");
}

export function expectedManifest(root, kind, profile, targetOverride, options = {}) {
  const packageInputs = packageInputsFingerprint(root, kind);
  const cargoLock = join(root, "Cargo.lock");
  const toolchain = join(root, "rust-toolchain.toml");
  const injectedGit =
    process.env.JAZZ_ARTIFACT_GIT_HEAD &&
    process.env.JAZZ_ARTIFACT_GIT_TREE &&
    process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF;
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
      head: injectedGit
        ? process.env.JAZZ_ARTIFACT_GIT_HEAD
        : run(root, "git", ["rev-parse", "HEAD"]),
      tree: injectedGit
        ? process.env.JAZZ_ARTIFACT_GIT_TREE
        : run(root, "git", ["rev-parse", "HEAD^{tree}"]),
      // Include staged, unstaged and untracked changes. A dirty build is valid
      // only for that exact dirty checkout, never merely for its HEAD commit.
      dirtyDiff: injectedGit
        ? process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF
        : sha256(
            `${run(root, "git", ["diff", "--binary", "HEAD", "--", ".", ":(exclude)crates/jazz-wasm/pkg/.jazz-artifact-manifest.json", ":(exclude)crates/jazz-wasm/.pkg-stage-*", ":(exclude)crates/jazz-wasm/.pkg-backup-*", ":(exclude)crates/jazz-wasm/.pkg-transaction.json*", ":(exclude)crates/jazz-wasm/.jazz-correctness-test-artifacts.json", ":(exclude)crates/jazz-napi/.jazz-artifact-manifest.json", ":(exclude)crates/jazz-napi/native-binding.pointer.cjs", ":(exclude)crates/jazz-napi/correctness-native-binding.pointer.cjs", ":(exclude)crates/jazz-napi/native-binding.d.ts", ":(exclude)crates/jazz-napi/native-artifact-fingerprint.cjs", ":(exclude)crates/jazz-napi/native-loader.cjs", ":(exclude)crates/jazz-napi/.native-artifacts/**", ":(exclude)packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts", ":(exclude)packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts"])}\n${run(root, "git", ["status", "--porcelain=v1", "--untracked-files=all", "--", ".", ":(exclude)crates/jazz-wasm/pkg/.jazz-artifact-manifest.json", ":(exclude)crates/jazz-wasm/.pkg-stage-*", ":(exclude)crates/jazz-wasm/.pkg-backup-*", ":(exclude)crates/jazz-wasm/.pkg-transaction.json*", ":(exclude)crates/jazz-wasm/.jazz-correctness-test-artifacts.json", ":(exclude)crates/jazz-napi/.jazz-artifact-manifest.json", ":(exclude)crates/jazz-napi/native-binding.pointer.cjs", ":(exclude)crates/jazz-napi/correctness-native-binding.pointer.cjs", ":(exclude)crates/jazz-napi/native-binding.d.ts", ":(exclude)crates/jazz-napi/native-artifact-fingerprint.cjs", ":(exclude)crates/jazz-napi/native-loader.cjs", ":(exclude)crates/jazz-napi/.native-artifacts/**", ":(exclude)packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts", ":(exclude)packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts"])}`,
          ),
    },
    cargoLock: existsSync(cargoLock) ? sha256(readFileSync(cargoLock)) : "missing",
    rustToolchain: existsSync(toolchain) ? sha256(readFileSync(toolchain)) : "missing",
    tools,
    toolchainInputs: sha256(JSON.stringify(tools)),
    target:
      targetOverride ??
      (kind === "wasm"
        ? "wasm32-unknown-unknown"
        : (toolVersion(root, "rustc", ["-vV"]).match(/^host: (.+)$/m)?.[1] ?? "unknown")),
    features: "default",
    packageInputs,
    artifacts: artifactHashes(root, kind, options),
  };
}

/**
 * ABI identity for generated bindings. Unlike the transport protocol this
 * covers the exact package inputs plus the tracked package wrappers. Generated
 * napi-rs JS/declaration outputs are intentionally excluded from packageInputs:
 * the producer writes those only inside a staged generation, and the Rust input
 * closure already determines them. Including them would create a pre-build vs
 * post-build circular fingerprint.
 */
export function nativeArtifactFingerprint(root, kind, profile, targetOverride) {
  // Runtime compatibility is content-addressed by relevant producer inputs,
  // never by commit identity or provenance receipts. In particular, do not
  // derive this through `expectedManifest`: its git HEAD/tree fields must
  // remain useful for local freshness without making the generated expected
  // fingerprint self-referential when that expectation is committed.
  const packageInputs = packageInputsFingerprint(root, kind);
  const surface =
    kind === "napi"
      ? ["crates/jazz-napi/index.cjs", "crates/jazz-napi/index.mjs"]
      : ["packages/jazz-tools/src/types/jazz-wasm.d.ts"];
  const surfaceHash = createHash("sha256");
  for (const path of surface) {
    surfaceHash.update(`${path}\0`);
    surfaceHash.update(existsSync(join(root, path)) ? readFileSync(join(root, path)) : "missing");
    surfaceHash.update("\0");
  }
  return sha256(`${packageInputs}\0${surfaceHash.digest("hex")}`);
}

export const manifestPath = (root, kind) => {
  if (kind === "wasm") return join(root, "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json");
  const packageDir = join(root, "crates/jazz-napi");
  const pointer = join(packageDir, "native-binding.pointer.cjs");
  if (existsSync(pointer)) {
    const generation = /\.native-artifacts\/(generation-[A-Za-z0-9.-]+)\/index\.js/.exec(
      readFileSync(pointer, "utf8"),
    )?.[1];
    if (generation)
      return join(packageDir, ".native-artifacts", generation, ".jazz-artifact-manifest.json");
  }
  return join(packageDir, ".jazz-artifact-manifest.json");
};

export function writeManifest(root, kind, profile, targetOverride, options = {}) {
  const path =
    options.wasmPackageDir || options.napiManifestDir
      ? join(options.wasmPackageDir ?? options.napiManifestDir, ".jazz-artifact-manifest.json")
      : manifestPath(root, kind);
  const manifest = expectedManifest(root, kind, profile, targetOverride, options);
  manifest.nativeArtifactFingerprint = nativeArtifactFingerprint(
    root,
    kind,
    profile,
    targetOverride,
  );
  writeFileSync(path, `${JSON.stringify(manifest, null, 2)}\n`);
}

export function verifyManifest(root, kind, profile, targetOverride) {
  const path = manifestPath(root, kind);
  if (!existsSync(path)) return `manifest is missing (${path})`;
  let actual;
  try {
    actual = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return `manifest is invalid (${path})`;
  }
  const expected = expectedManifest(root, kind, profile, targetOverride);
  expected.nativeArtifactFingerprint = nativeArtifactFingerprint(
    root,
    kind,
    profile,
    targetOverride,
  );
  for (const key of [
    "schema",
    "kind",
    "profile",
    "cargoLock",
    "rustToolchain",
    "toolchainInputs",
    "target",
    "features",
    "packageInputs",
    "artifacts",
    "nativeArtifactFingerprint",
  ]) {
    if (JSON.stringify(actual[key]) !== JSON.stringify(expected[key]))
      return `${key} differs (built ${JSON.stringify(actual[key])}, expected ${JSON.stringify(expected[key])})`;
  }
  for (const key of ["rustc", "wasmPack", "wasmBindgen", "wasmOpt", "napi"]) {
    if (actual.tools?.[key] !== expected.tools[key])
      return `tools.${key} differs (built ${JSON.stringify(actual.tools?.[key])}, expected ${JSON.stringify(expected.tools[key])})`;
  }
  for (const key of ["head", "tree", "dirtyDiff"])
    if (actual.git?.[key] !== expected.git[key]) return `git.${key} differs`;
  return null;
}

export function verifyPublishedNapiManifest(manifest, target, nodePath) {
  if (manifest.kind !== "napi" || manifest.profile !== "release" || manifest.target !== target)
    return `manifest is for ${manifest.kind}/${manifest.profile}/${manifest.target}, expected napi/release/${target}`;
  if (!existsSync(nodePath)) return `native binding is missing (${nodePath})`;
  const expected = { file: basename(nodePath), sha256: sha256(readFileSync(nodePath)) };
  return manifest.artifacts?.some(
    (artifact) => artifact.file === expected.file && artifact.sha256 === expected.sha256,
  )
    ? null
    : `manifest does not match ${expected.file}`;
}

function main(args) {
  const [command, kind, profile] = args;
  const rootFlag = args.indexOf("--root");
  const root = rootFlag === -1 ? here : resolve(args[rootFlag + 1]);
  const targetFlag = args.indexOf("--target");
  const target = targetFlag === -1 ? undefined : args[targetFlag + 1];
  if (!command || !kind || !profile || !["wasm", "napi"].includes(kind))
    throw new Error("usage: provenance.mjs <write|verify> <wasm|napi> <profile> [--root path]");
  if (command === "write") {
    writeManifest(root, kind, profile, target);
    return;
  }
  if (command === "verify") {
    const problem = verifyManifest(root, kind, profile, target);
    if (problem) {
      console.error(`STALE ${kind} ${profile}: ${problem}`);
      process.exitCode = 1;
    } else console.log(`FRESH ${kind} ${profile}`);
    return;
  }
  throw new Error(`unknown command: ${command}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(`artifact provenance: ${error.message}`);
    process.exitCode = 2;
  }
}

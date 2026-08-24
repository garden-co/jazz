#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, renameSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { basename, join, resolve } from "node:path";
import { writeManifest } from "./provenance.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const commands = {
  wasm: {
    fast: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--dev"]],
    release: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--release"]],
    profiling: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--profiling"]],
  },
  napi: {
    debug: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform"]],
    release: [
      "pnpm",
      ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--release"],
    ],
    perf: [
      "pnpm",
      ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--profile", "perf"],
    ],
  },
};

const wasmPackageFiles = [
  "jazz_wasm_bg.wasm",
  "jazz_wasm_bg.wasm.d.ts",
  "jazz_wasm.d.ts",
  "jazz_wasm.js",
  "package.json",
];

/**
 * wasm-pack writes its wasm-opt scratch output beside its final package. Keep
 * every producer in a unique directory so a concurrent profile/build cannot
 * delete another producer's intermediate before wasm-opt consumes it.
 */
export function createWasmPackageStage(rootDir = root, profile = "release") {
  const packageDir = join(rootDir, "crates", "jazz-wasm");
  const path = mkdtempSync(join(packageDir, `.pkg-${profile}-`));
  return { path, outDir: basename(path) };
}

export function assertCompleteWasmPackage(path) {
  const missing = wasmPackageFiles.filter((file) => !existsSync(join(path, file)));
  if (missing.length) {
    throw new Error(
      `WASM build produced an incomplete staged package (${basename(path)}; missing ${missing.join(", ")})`,
    );
  }
}

/**
 * Replace only complete final files. Each rename is atomic, and a failed
 * wasm-pack invocation never touches the shared package consumers import.
 */
export function publishWasmPackage(stagePath, packagePath) {
  try {
    assertCompleteWasmPackage(stagePath);
    mkdirSync(packagePath, { recursive: true });
    for (const file of wasmPackageFiles) renameSync(join(stagePath, file), join(packagePath, file));
  } catch (error) {
    throw new Error(
      `WASM package publish failed from staged package ${basename(stagePath)}: ${error.message}`,
    );
  } finally {
    rmSync(stagePath, { recursive: true, force: true });
  }
}

export function buildArtifact(kind, profile = "release", extraArgs = []) {
  const selected = commands[kind]?.[profile];
  if (!selected)
    throw new Error("usage: build.mjs <wasm fast|release|profiling | napi debug|release>");
  const [command, selectedArgs] = selected;
  const args = [...selectedArgs, ...extraArgs];
  if (kind !== "napi" && extraArgs.length)
    throw new Error("only napi builds accept extra napi CLI arguments");
  const targetIndex = extraArgs.indexOf("--target");
  const target = targetIndex === -1 ? undefined : extraArgs[targetIndex + 1];
  const wasmStage = kind === "wasm" ? createWasmPackageStage(root, profile) : undefined;
  if (wasmStage) args.push("--out-dir", wasmStage.outDir);
  const napiBindingForTarget = {
    "x86_64-unknown-linux-gnu": "jazz-napi.linux-x64-gnu.node",
    "x86_64-pc-windows-msvc": "jazz-napi.win32-x64-msvc.node",
    "x86_64-apple-darwin": "jazz-napi.darwin-x64.node",
    "aarch64-apple-darwin": "jazz-napi.darwin-arm64.node",
  };
  const hostTarget = {
    "linux-x64": "x86_64-unknown-linux-gnu",
    "win32-x64": "x86_64-pc-windows-msvc",
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
  }[`${process.platform}-${process.arch}`];
  const resolvedNapiTarget = target ?? hostTarget;
  const expectedNapiBinding = kind === "napi" && napiBindingForTarget[resolvedNapiTarget];
  if (kind === "napi" && !expectedNapiBinding)
    throw new Error(`unsupported NAPI target ${resolvedNapiTarget ?? "unknown"}`);
  const napiPath = expectedNapiBinding && join(root, "crates/jazz-napi", expectedNapiBinding);
  const stagedNapiPath = napiPath && `${napiPath}.staged-${process.pid}-${Date.now()}`;
  if (napiPath && existsSync(napiPath)) renameSync(napiPath, stagedNapiPath);
  const result = spawnSync(command, args, {
    cwd: root,
    stdio: "inherit",
    shell: process.platform === "win32",
  });
  const restoreStagedNapi = () => {
    if (stagedNapiPath && existsSync(stagedNapiPath)) {
      if (!existsSync(napiPath)) renameSync(stagedNapiPath, napiPath);
      else rmSync(stagedNapiPath, { force: true });
    }
  };
  if (result.error) {
    if (wasmStage) rmSync(wasmStage.path, { recursive: true, force: true });
    restoreStagedNapi();
    throw result.error;
  }
  if (result.status !== 0) {
    if (wasmStage) {
      rmSync(wasmStage.path, { recursive: true, force: true });
      throw new Error(
        `WASM ${profile} build failed before publishing staged package ${basename(wasmStage.path)}; the prior package remains intact`,
      );
    }
    restoreStagedNapi();
    process.exitCode = result.status ?? 1;
    return;
  }
  if (wasmStage) publishWasmPackage(wasmStage.path, join(root, "crates", "jazz-wasm", "pkg"));
  if (napiPath && !existsSync(napiPath)) {
    restoreStagedNapi();
    throw new Error(`NAPI build produced no ${expectedNapiBinding}; refusing to write provenance`);
  }
  if (napiPath && resolvedNapiTarget === hostTarget) {
    const load = spawnSync(process.execPath, ["-e", "require(process.argv[1])", napiPath], {
      stdio: "inherit",
    });
    if ((load.status ?? 1) !== 0) {
      rmSync(napiPath, { force: true });
      restoreStagedNapi();
      throw new Error(
        "NAPI build produced an unloadable host binding; refusing to write provenance",
      );
    }
  }
  if (stagedNapiPath) rmSync(stagedNapiPath, { force: true });
  writeManifest(root, kind, profile, target);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [kind, profile = "release", ...extraArgs] = process.argv.slice(2);
  buildArtifact(kind, profile, extraArgs);
}

#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { existsSync, renameSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";
import { writeManifest } from "./provenance.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const [kind, profile = "release", ...extraArgs] = process.argv.slice(2);
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
const selected = commands[kind]?.[profile];
if (!selected)
  throw new Error("usage: build.mjs <wasm fast|release|profiling | napi debug|release>");
const [command, args] = selected;
if (kind !== "napi" && extraArgs.length)
  throw new Error("only napi builds accept extra napi CLI arguments");
args.push(...extraArgs);
const targetIndex = extraArgs.indexOf("--target");
const target = targetIndex === -1 ? undefined : extraArgs[targetIndex + 1];
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
  restoreStagedNapi();
  throw result.error;
}
if (result.status !== 0) {
  restoreStagedNapi();
  process.exit(result.status ?? 1);
}
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
    throw new Error("NAPI build produced an unloadable host binding; refusing to write provenance");
  }
}
if (stagedNapiPath) rmSync(stagedNapiPath, { force: true });
writeManifest(root, kind, profile, target);

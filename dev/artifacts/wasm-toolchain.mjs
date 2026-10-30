import { spawnSync } from "node:child_process";
import { join } from "node:path";

// Target-specific overrides, in cc-rs precedence order.
export const wasmArchiverVariables = ["AR_wasm32-unknown-unknown", "AR_wasm32_unknown_unknown"];

export function wasmBuildEnvironment(
  env = process.env,
  { platform = process.platform, run = spawnSync } = {},
) {
  // A native archiver from the shell must not override the WASM toolchain.
  // Copy first so native builds keep their original environment.
  env = { ...env };
  delete env.AR;
  delete env.TARGET_AR;
  if (platform !== "darwin" || wasmArchiverVariables.some((key) => env[key] !== undefined))
    return { ...env };

  const probe = (command, args) => run(command, args, { env, encoding: "utf8", timeout: 10_000 });
  const isLlvmArchiver = (command) => {
    const result = probe(command, ["--version"]);
    return !result.error && result.status === 0 && /LLVM/i.test(result.stdout ?? "");
  };

  let archiver = "llvm-ar";
  if (!isLlvmArchiver(archiver)) {
    const brew = probe("brew", ["--prefix", "llvm"]);
    const prefix = brew.stdout?.trim();
    archiver = !brew.error && brew.status === 0 && prefix ? join(prefix, "bin", "llvm-ar") : null;
    if (!archiver || !isLlvmArchiver(archiver))
      throw new Error(
        "WASM builds on macOS require LLVM's archiver to bundle SQLite. " +
          "Install it with `brew install llvm`, then rerun the build. " +
          "For a custom installation, set AR_wasm32_unknown_unknown to its llvm-ar executable. " +
          "Apple's ar/ranlib cannot archive these WebAssembly objects.",
      );
  }
  // Scope the selection to WASM; native builds retain Apple's toolchain.
  return { ...env, AR_wasm32_unknown_unknown: archiver };
}

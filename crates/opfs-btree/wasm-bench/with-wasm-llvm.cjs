#!/usr/bin/env node

const { execFileSync, spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");

function brewPrefix(formula) {
  try {
    return execFileSync("brew", ["--prefix", formula], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  } catch {
    return "";
  }
}

function clangSupportsWasm(clang) {
  if (!fs.existsSync(clang)) return false;
  const result = spawnSync(clang, ["--print-targets"], { encoding: "utf8" });
  return result.status === 0 && /wasm32/i.test(result.stdout);
}

function findLlvmPrefix() {
  const candidates = [
    process.env.LLVM_PREFIX,
    brewPrefix("llvm"),
    brewPrefix("llvm@20"),
    brewPrefix("llvm@19"),
    "/opt/homebrew/opt/llvm",
    "/opt/homebrew/opt/llvm@20",
    "/opt/homebrew/opt/llvm@19",
    "/usr/local/opt/llvm",
  ].filter(Boolean);

  for (const prefix of candidates) {
    if (clangSupportsWasm(path.join(prefix, "bin", "clang"))) return prefix;
  }
  return "";
}

const [command, ...args] = process.argv.slice(2);
if (!command) {
  console.error("usage: with-wasm-llvm.cjs <command> [...args]");
  process.exit(2);
}

const llvmPrefix = findLlvmPrefix();
if (!llvmPrefix) {
  console.error("error: no clang with the wasm32 target found.");
  console.error("Install Homebrew LLVM, for example: brew install llvm");
  process.exit(1);
}

const env = { ...process.env };
delete env.NO_COLOR;
env.CC_wasm32_unknown_unknown = path.join(llvmPrefix, "bin", "clang");
env.AR_wasm32_unknown_unknown = path.join(llvmPrefix, "bin", "llvm-ar");
env.CFLAGS_wasm32_unknown_unknown = "-O3 -DSQLITE_THREADSAFE=0";

const result = spawnSync(command, args, { env, stdio: "inherit" });
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);

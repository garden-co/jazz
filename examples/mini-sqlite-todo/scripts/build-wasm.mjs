import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const exampleDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(exampleDir, "../..");
const crateDir = path.join(repoRoot, "crates/mini-jazz-sqlite-wasm");
const outDir = path.join(exampleDir, "src/generated/mini-jazz-sqlite-wasm");

const env = { ...process.env };
if (!env.CC_wasm32_unknown_unknown) {
  const clangCandidates = [
    "/opt/homebrew/opt/llvm/bin/clang",
    "/opt/homebrew/opt/llvm@21/bin/clang",
    "/usr/local/opt/llvm/bin/clang",
  ];
  const clang = clangCandidates.find((candidate) => existsSync(candidate));
  if (clang) {
    env.CC_wasm32_unknown_unknown = clang;
  }
}

const result = spawnSync(
  "wasm-pack",
  ["build", crateDir, "--target", "web", "--out-dir", outDir, "--profiling"],
  {
    cwd: repoRoot,
    env,
    stdio: "inherit",
  },
);

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);

const { spawnSync } = require("node:child_process");
const { readdirSync } = require("node:fs");
const { join } = require("node:path");

const release = !process.argv.includes("--debug") && process.env.JAZZ_NAPI_RELEASE !== "0";
const args = ["build", "--platform"];
if (release) {
  args.push("--release");
}

const result = spawnSync("napi", args, {
  stdio: "inherit",
  shell: process.platform === "win32",
});

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}

if ((result.status ?? 1) !== 0) {
  process.exit(result.status ?? 1);
}

// macOS Tahoe (26.x) rejects the 4K-page ad-hoc signature emitted by Apple
// `ld` for Rust dylibs — dyld validates code pages at the kernel's 16K
// granularity and kills the load as "Invalid Page". Re-sign with `codesign`
// to produce 16K-page hashes the kernel accepts.
if (process.platform === "darwin") {
  const cwd = join(__dirname, "..");
  const node = readdirSync(cwd).filter((f) => f.endsWith(".node"));
  for (const file of node) {
    const sign = spawnSync("codesign", ["--force", "--sign", "-", join(cwd, file)], {
      stdio: "inherit",
    });
    if ((sign.status ?? 1) !== 0) {
      process.exit(sign.status ?? 1);
    }
  }
}

process.exit(0);

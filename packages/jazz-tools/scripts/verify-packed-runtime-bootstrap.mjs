import { spawnSync } from "node:child_process";
import { accessSync, chmodSync, constants, existsSync } from "node:fs";
import { join, resolve } from "node:path";

const BINARIES = {
  "darwin-arm64": "jazz-tools-darwin-arm64",
  "darwin-x64": "jazz-tools-darwin-x64",
  "linux-arm64": "jazz-tools-linux-arm64",
  "linux-x64": "jazz-tools-linux-x64",
  "win32-x64": "jazz-tools-windows-x64.exe",
};

function fail(message) {
  console.error(message);
  process.exit(1);
}

const packageDirArg = process.argv[2];

if (!packageDirArg) {
  fail("Usage: node scripts/verify-packed-runtime-bootstrap.mjs <packed-package-dir>");
}

const packageDir = resolve(packageDirArg);
const wrapperPath = join(packageDir, "bin", "jazz-tools.js");
const binaryName = BINARIES[`${process.platform}-${process.arch}`];

if (!binaryName) {
  fail(
    `Unsupported host platform for packed runtime bootstrap verification: ${process.platform}/${process.arch}`,
  );
}

const binaryPath = join(packageDir, "bin", "native", binaryName);

if (!existsSync(wrapperPath)) {
  fail(`Packed wrapper missing: ${wrapperPath}`);
}

if (!existsSync(binaryPath)) {
  fail(`Packed host binary missing: ${binaryPath}`);
}

if (process.platform !== "win32") {
  // Simulate the mode we get after round-tripping through GitHub Actions artifacts.
  chmodSync(binaryPath, 0o644);
}

const result = spawnSync(process.execPath, [wrapperPath, "create", "--help"], {
  encoding: "utf8",
  env: process.env,
});

if (result.error) {
  fail(`Failed to launch packed wrapper: ${result.error.message}`);
}

if (result.status !== 0) {
  const stderr = result.stderr?.trim();
  const stdout = result.stdout?.trim();
  const details = [stderr, stdout].filter(Boolean).join("\n");
  fail(
    details
      ? `Packed runtime bootstrap probe failed.\n${details}`
      : "Packed runtime bootstrap probe failed without output.",
  );
}

if (process.platform !== "win32") {
  try {
    accessSync(binaryPath, constants.X_OK);
  } catch {
    fail(`Packed native binary ${binaryName} is not executable after runtime bootstrap.`);
  }
}

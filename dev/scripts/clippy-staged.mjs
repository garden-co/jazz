// Scoped clippy: maps changed files to their owning crate via
// and runs `cargo clippy --package <crate>` for each affected
// crate instead of the whole workspace. Falls back to a full
// --workspace run when a workspace-root file (Cargo.toml/Cargo.lock)
// changes

import { execFileSync, spawnSync } from "node:child_process";
import path from "node:path";

const workspaceRoot = process.cwd();
const dryRun = process.argv[2] === "--dry-run";

const stagedFiles = dryRun
  ? process.argv.slice(3)
  : execFileSync("git", ["diff", "--cached", "--name-only", "--diff-filter=ACMRD", "-z"], {
      encoding: "buffer",
    })
      .toString("utf8")
      .split("\0")
      .filter(Boolean);

const clippyInputs = stagedFiles.filter(
  (file) => file.endsWith(".rs") || path.basename(file) === "Cargo.toml",
);

if (clippyInputs.length === 0) {
  console.log("Clippy: no staged Rust or Cargo.toml changes");
  process.exit(0);
}

const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version", "1"], { encoding: "utf8" }),
);
const workspaceMemberIds = new Set(metadata.workspace_members);
const workspacePackages = metadata.packages
  .filter((pkg) => workspaceMemberIds.has(pkg.id))
  .map((pkg) => ({
    name: pkg.name,
    directory: path.dirname(pkg.manifest_path),
  }))
  .sort((left, right) => right.directory.length - left.directory.length);

const rootManifest = path.join(workspaceRoot, "Cargo.toml");
const packages = new Set();
let lintWorkspace = false;

for (const file of clippyInputs) {
  const absoluteFile = path.resolve(workspaceRoot, file);
  if (absoluteFile === rootManifest) {
    lintWorkspace = true;
    break;
  }

  const owningPackage = workspacePackages.find(({ directory }) => {
    const relativePath = path.relative(directory, absoluteFile);
    return (
      relativePath === "" ||
      (!relativePath.startsWith(`..${path.sep}`) &&
        relativePath !== ".." &&
        !path.isAbsolute(relativePath))
    );
  });

  if (owningPackage) {
    packages.add(owningPackage.name);
  } else {
    // Preserve the old hook's coverage for Rust files outside known packages.
    lintWorkspace = true;
    break;
  }
}

const cargoArgs = ["clippy"];
if (lintWorkspace) {
  cargoArgs.push("--workspace");
} else {
  for (const packageName of [...packages].sort()) {
    cargoArgs.push("--package", packageName);
  }
}
cargoArgs.push("--", "-D", "warnings");

console.log(`Clippy: cargo ${cargoArgs.join(" ")}`);
if (dryRun) {
  process.exit(0);
}

const result = spawnSync("cargo", cargoArgs, { stdio: "inherit" });
if (result.error) {
  console.error(`Failed to run Cargo: ${result.error.message}`);
  process.exit(1);
}
process.exit(result.status ?? 1);

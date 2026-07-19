const { spawnSync } = require("node:child_process");

const release = !process.argv.includes("--debug") && process.env.JAZZ_NAPI_RELEASE !== "0";
const profileIndex = process.argv.indexOf("--profile");
const profile = profileIndex === -1 ? undefined : process.argv[profileIndex + 1];
const args = ["build", "--platform"];
if (profile) {
  args.push("--profile", profile);
} else if (release) {
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
process.exit(0);

// Cache-salt 2026-07-19: a corrupt turbo cache archive for jazz-napi#build
// reproducibly restored the package without a loadable .node on CI
// (see dev/CI_NOTES.md). Changing this file busts the poisoned key.

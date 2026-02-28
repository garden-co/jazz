const { spawnSync } = require("node:child_process");

const release = process.env.JAZZ_NAPI_RELEASE !== "0";
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

process.exit(result.status ?? 1);

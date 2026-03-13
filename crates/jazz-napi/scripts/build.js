const { spawnSync } = require("node:child_process");
const path = require("node:path");

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

const ensureScopedLoader = spawnSync(
  process.execPath,
  [path.join(__dirname, "ensure-scoped-loader.js")],
  {
    stdio: "inherit",
    shell: process.platform === "win32",
  },
);

if (ensureScopedLoader.error) {
  console.error(ensureScopedLoader.error.message);
  process.exit(1);
}

process.exit(ensureScopedLoader.status ?? 1);

const { spawnSync } = require("node:child_process");
const { resolve } = require("node:path");

const release = !process.argv.includes("--debug") && process.env.JAZZ_NAPI_RELEASE !== "0";
const profileIndex = process.argv.indexOf("--profile");
const profile = profileIndex === -1 ? undefined : process.argv[profileIndex + 1];
const artifactProfile = profile ?? (release ? "release" : "debug");
const args = [resolve(__dirname, "../../../dev/artifacts/build.mjs"), "napi", artifactProfile];
const result = spawnSync(process.execPath, args, { stdio: "inherit" });
if (result.error) throw result.error;
process.exit(result.status ?? 1);

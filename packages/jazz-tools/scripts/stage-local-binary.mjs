import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";
import { stageBinary } from "./stage-binary.mjs";

const thisFile = fileURLToPath(import.meta.url);
const scriptsDir = dirname(thisFile);
const packageDir = resolve(scriptsDir, "..");
const repoRoot = resolve(packageDir, "..", "..");

const sourceBinary = join(
  repoRoot,
  "target",
  "release",
  process.platform === "win32" ? "jazz-tools.exe" : "jazz-tools",
);

stageBinary({
  source: sourceBinary,
  platform: process.platform,
  arch: process.arch,
})
  .then(() => {
    console.log("Local binary staged for current platform.");
  })
  .catch((error) => {
    console.error(error.message);
    process.exit(1);
  });

import { chmod, copyFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { TARGETS, keyFor } from "./targets.mjs";

const thisFile = fileURLToPath(import.meta.url);
const scriptsDir = dirname(thisFile);
const packageDir = resolve(scriptsDir, "..");
const nativeDir = join(packageDir, "bin", "native");

function parseArgs(argv) {
  const parsed = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    parsed[key] = value;
    i += 1;
  }
  return parsed;
}

export async function stageBinary({ source, platform, arch }) {
  const targetKey = keyFor(platform, arch);
  const fileName = TARGETS[targetKey];
  if (!fileName) {
    throw new Error(`Unsupported target for npm bundle: ${platform}/${arch}`);
  }

  const sourcePath = resolve(source);
  if (!existsSync(sourcePath)) {
    throw new Error(`Source binary does not exist: ${sourcePath}`);
  }

  await mkdir(nativeDir, { recursive: true });
  const destination = join(nativeDir, fileName);
  await copyFile(sourcePath, destination);

  if (!fileName.endsWith(".exe")) {
    await chmod(destination, 0o755);
  }

  console.log(`Staged ${destination}`);
}

async function main() {
  const args = parseArgs(process.argv);
  const source = args.source;
  const platform = args.platform;
  const arch = args.arch;

  if (!source || !platform || !arch) {
    throw new Error(
      "Usage: node scripts/stage-binary.mjs --source <path> --platform <platform> --arch <arch>",
    );
  }

  await stageBinary({ source, platform, arch });
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : null;
if (invokedPath && fileURLToPath(import.meta.url) === invokedPath) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

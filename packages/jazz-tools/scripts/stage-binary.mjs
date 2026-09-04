import { chmod, copyFile, mkdir, realpath, rm, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { TARGETS, keyFor } from "./targets.mjs";

const thisFile = fileURLToPath(import.meta.url);
const scriptsDir = dirname(thisFile);
const packageDir = resolve(scriptsDir, "..");
const defaultNativeDir = join(packageDir, "bin", "native");

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

export async function stageBinary({ source, platform, arch, nativeDir = defaultNativeDir }) {
  const targetKey = keyFor(platform, arch);
  const fileName = TARGETS[targetKey];
  if (!fileName) {
    throw new Error(`Unsupported target for npm bundle: ${platform}/${arch}`);
  }

  const sourcePath = resolve(source);
  const destination = resolve(nativeDir, fileName);
  let sourceStats;
  try {
    sourceStats = await stat(sourcePath, { bigint: true });
  } catch (error) {
    if (error?.code === "ENOENT") {
      throw new Error(`Source binary does not exist: ${sourcePath}`);
    }
    throw error;
  }
  if (!sourceStats.isFile()) {
    throw new Error(`Source binary is not a file: ${sourcePath}`);
  }

  await mkdir(nativeDir, { recursive: true });

  let destinationStats;
  try {
    destinationStats = await stat(destination, { bigint: true });
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const sameCanonicalPath =
    destinationStats !== undefined &&
    (await realpath(sourcePath)) === (await realpath(destination));
  const hasStableIdentity =
    sourceStats.ino !== 0n && destinationStats !== undefined && destinationStats.ino !== 0n;
  const sourceAndDestinationMatch =
    sourcePath === destination ||
    sameCanonicalPath ||
    (hasStableIdentity &&
      sourceStats.dev === destinationStats.dev &&
      sourceStats.ino === destinationStats.ino);

  if (sourceAndDestinationMatch) {
    if (!fileName.endsWith(".exe")) {
      await chmod(destination, 0o755);
    }
    console.log(`Staged ${destination}`);
    return;
  }

  // Remove first: overwriting in place keeps the vnode, and the macOS
  // kernel SIGKILLs a cached executable whose content changed under it.
  await rm(destination, { force: true });
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

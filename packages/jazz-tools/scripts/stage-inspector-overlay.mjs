// Copy the inspector overlay's embedded build into jazz-tools' own dist so the
// published package is self-contained (no jazz-inspector dependency). The dev
// server's resolveEmbeddedDir() reads it from this exact spot at runtime —
// keeping the destination here, next to that module, means the two agree.
// Run from the workspace (`pnpm --filter jazz-tools run stage:inspector-overlay`)
// after the inspector's embedded build to reproduce the published layout locally.
import { cp, lstat, mkdir, mkdtemp, readdir, rename, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

async function pathExists(path) {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
}

async function validateOverlay(directory) {
  const embeddedPath = join(directory, "embedded.html");
  const embeddedStat = await lstat(embeddedPath);
  if (!embeddedStat.isFile()) {
    throw new Error("Inspector overlay embedded.html must be a regular file");
  }

  async function rejectUnsafeEntries(path) {
    for (const entry of await readdir(path, { withFileTypes: true })) {
      const entryPath = join(path, entry.name);
      if (entry.isSymbolicLink()) {
        throw new Error(`Inspector overlay must not contain symlinks: ${entry.name}`);
      }
      if (entry.isDirectory()) {
        await rejectUnsafeEntries(entryPath);
      } else if (!entry.isFile()) {
        throw new Error(`Inspector overlay contains an unsupported entry: ${entry.name}`);
      }
    }
  }

  await rejectUnsafeEntries(directory);
}

const here = dirname(fileURLToPath(import.meta.url)); // packages/jazz-tools/scripts
const src = join(here, "../../inspector/dist-embedded");
const dest = join(here, "../dist/dev/inspector-overlay/embedded");
const parent = dirname(dest);
await mkdir(parent, { recursive: true });
const backup = join(parent, ".inspector-overlay-backup");
// Promotion uses two renames, so interruption can briefly leave dest absent; the next run recovers backup.
let destinationExists = await pathExists(dest);
if (!destinationExists && (await pathExists(backup))) {
  // Recover an interrupted promotion before attempting to stage a replacement.
  await rename(backup, dest);
  destinationExists = true;
} else if (destinationExists && (await pathExists(backup))) {
  // A completed promotion may have been interrupted before deleting its old backup.
  await rm(backup, { recursive: true, force: true });
}
const stage = await mkdtemp(join(parent, ".inspector-overlay-stage-"));
let primaryError;

try {
  await cp(src, stage, { recursive: true });
  await validateOverlay(stage);

  if (destinationExists) await rename(dest, backup);

  try {
    await rename(stage, dest);
  } catch (promotionError) {
    if (destinationExists) {
      try {
        await rename(backup, dest);
      } catch (rollbackError) {
        throw new Error(
          `Could not promote inspector overlay and rollback failed; backup retained at ${backup}`,
          { cause: new AggregateError([promotionError, rollbackError]) },
        );
      }
    }
    throw promotionError;
  }

  if (destinationExists) await rm(backup, { recursive: true, force: true });
  console.log(`Staged inspector overlay assets → ${dest}`);
} catch (error) {
  primaryError = error;
  throw error;
} finally {
  try {
    await rm(stage, { recursive: true, force: true });
  } catch (cleanupError) {
    if (!primaryError) throw cleanupError;
  }
}

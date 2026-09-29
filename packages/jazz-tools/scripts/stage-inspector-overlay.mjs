// Copy the inspector overlay's embedded build into jazz-tools' own dist so the
// published package is self-contained (no jazz-inspector dependency). The dev
// server's resolveEmbeddedDir() reads it from this exact spot at runtime —
// keeping the destination here, next to that module, means the two agree.
// Run from the workspace (`pnpm --filter jazz-tools run stage:inspector-overlay`)
// after the inspector's embedded build to reproduce the published layout locally.
import { access, cp, lstat, mkdir, mkdtemp, rename, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url)); // packages/jazz-tools/scripts
const src = join(here, "../../inspector/dist-embedded");
const dest = join(here, "../dist/dev/inspector-overlay/embedded");
const parent = dirname(dest);
await mkdir(parent, { recursive: true });
const stage = await mkdtemp(join(parent, ".inspector-overlay-stage-"));
const backup = join(parent, `.inspector-overlay-backup-${randomUUID()}`);
let primaryError;

try {
  await cp(src, stage, { recursive: true });
  await access(join(stage, "embedded.html"));

  let destinationExists = true;
  try {
    await lstat(dest);
  } catch (error) {
    if (error.code === "ENOENT") destinationExists = false;
    else throw error;
  }

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

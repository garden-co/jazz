import { readdirSync, rmSync } from "node:fs";
import { basename, dirname, join } from "node:path";

function processIsAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === "EPERM") return true;
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

/**
 * Remove native bindings parked by interrupted builds before provenance is
 * measured. A live owner is never deleted: direct builds do not necessarily
 * share the higher-level artifact lock, so concurrent publication fails
 * closed instead of corrupting either build.
 */
export function removeAbandonedNapiStages(napiPath, ownerIsAlive = processIsAlive) {
  const directory = dirname(napiPath);
  const stagedPrefix = `${basename(napiPath)}.staged-`;
  for (const entry of readdirSync(directory)) {
    if (!entry.startsWith(stagedPrefix)) continue;
    const owner = Number.parseInt(entry.slice(stagedPrefix.length).split("-", 1)[0], 10);
    if (Number.isInteger(owner) && owner > 0 && ownerIsAlive(owner)) {
      throw new Error(`another NAPI build retains ${entry}; wait for process ${owner}`);
    }
    rmSync(join(directory, entry), { force: true });
  }
}

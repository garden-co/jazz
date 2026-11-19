import type { WasmSchema } from "../drivers/types.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";

/**
 * How a tab's worker fingerprint (`createBrowserWorkerFingerprint`) relates to
 * the store's open context:
 *
 * - `"admit"`: the tab joins it.
 * - `"held-by-another-sign-in"`: the store is the account's either way (the
 *   storage owner already matched) but open under another sign-in. The tab
 *   waits for that sign-in to go (another tab follows a sign-up or sign-out,
 *   or closes).
 * - `"held-by-another-build"`: the store is open with another schema, and
 *   only that differs: another tab runs another build of the app (one
 *   deployed before or after this one). The tab waits for that tab to reload
 *   or close.
 * - `"incompatible"`: any other configuration differs (another worker asset,
 *   protocol or storage format, app, environment or telemetry).
 *
 * A tab that hasn't chosen a schema (its `Db` hasn't read or written, and
 * opens the store only to delete it, as a `clearOnSignOut` sign-out does)
 * needs none of the store's: it joins whatever schema the store is open
 * with. The worker's runtime serves the schema of the tab that opened it.
 */
export function compareRuntimeAdmission(
  open: string,
  requested: string,
  requestedSchema: WasmSchema,
): "admit" | "held-by-another-sign-in" | "held-by-another-build" | "incompatible" {
  if (open === requested) return "admit";
  try {
    const {
      authClass: openAuth,
      schemaHash: openSchema,
      ...openRest
    } = JSON.parse(open) as Record<string, unknown>;
    const {
      authClass: requestedAuth,
      schemaHash: requestedSchemaHash,
      ...requestedRest
    } = JSON.parse(requested) as Record<string, unknown>;
    if (JSON.stringify(openRest) !== JSON.stringify(requestedRest)) return "incompatible";
    const schemaless = Object.keys(requestedSchema).length === 0;
    if (openSchema !== requestedSchemaHash && !schemaless) return "held-by-another-build";
    return openAuth === requestedAuth ? "admit" : "held-by-another-sign-in";
  } catch {
    return "incompatible";
  }
}

/**
 * Which of two builds of the app is newer, by the schema history each ships
 * (`ClientMigrations`, every version it knows): a build whose history
 * strictly contains the other's came later. Two that ship the same history,
 * or histories neither contains (unrelated builds, a rollback), or a build
 * that ships none, can't be ordered: `"unknown"`, and the tab of the other
 * build waits, as before.
 */
export function compareBuildLineage(
  open: ClientMigrations | undefined,
  requested: ClientMigrations | undefined,
): "newer" | "older" | "unknown" {
  const versions = (lineage: ClientMigrations | undefined) =>
    new Set((lineage?.schemas ?? []).map((version) => version.hash));
  const openVersions = versions(open);
  const requestedVersions = versions(requested);
  if (openVersions.size === 0 || requestedVersions.size === 0) return "unknown";
  const contains = (outer: Set<string>, inner: Set<string>) =>
    outer.size > inner.size && [...inner].every((hash) => outer.has(hash));
  if (contains(requestedVersions, openVersions)) return "newer";
  if (contains(openVersions, requestedVersions)) return "older";
  return "unknown";
}

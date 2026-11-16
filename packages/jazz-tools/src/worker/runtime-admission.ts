import type { WasmSchema } from "../drivers/types.js";

/**
 * How a tab's worker fingerprint (`createBrowserWorkerFingerprint`) relates to
 * the store's open context:
 *
 * - `"admit"`: the tab joins it.
 * - `"held-by-another-sign-in"`: the store is the account's either way (the
 *   storage owner already matched) but open under another sign-in. The tab
 *   waits for that sign-in to go (another tab follows a sign-up or sign-out,
 *   or closes).
 * - `"incompatible"`: another configuration (an older deploy's schema).
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
): "admit" | "held-by-another-sign-in" | "incompatible" {
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
    if (openSchema !== requestedSchemaHash && !schemaless) return "incompatible";
    return openAuth === requestedAuth ? "admit" : "held-by-another-sign-in";
  } catch {
    return "incompatible";
  }
}

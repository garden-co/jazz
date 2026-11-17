/**
 * Enum columns with cases (`s.enum({ bug: { severity: s.int() }, … })`,
 * `{ "enum": { "bug": { … }, … } }` in jazz.app.json) are removed in
 * alpha.65, and its loaders refuse an app whose current tables declare one.
 * No alpha.64 store could hold a value of one either: every write failed
 * (jazz#5621). So upgrade refuses such an app, naming each column, rather
 * than write files the loader then refuses.
 *
 * Older versions are another matter: a store keeps every version it admitted,
 * and the loader reads an enum with cases in `history` and in migrations'
 * `add`/`drop` types as a legacy type that hashes as alpha.64 did. Those pass
 * through unchanged, so an app that has since moved off one can upgrade.
 */
import { entriesOf, isObject, type Json, type JsonObject } from "./app-json.js";
import { UpgradeRefusal } from "./errors.js";

/** `table.column` of every enum column with cases in the current tables (not history or migrations). */
export function payloadEnumColumns(app: JsonObject): string[] {
  const found: string[] = [];
  if (!isObject(app.tables)) return found;
  for (const [table, spec] of entriesOf(app.tables)) {
    const columns = isObject(spec) ? spec.columns : undefined;
    if (!isObject(columns)) continue;
    for (const [column, type] of entriesOf(columns)) {
      if (hasCases(type)) found.push(`${table}.${column}`);
    }
  }
  return found;
}

/** A column type that is, or holds (as an array's element), an enum with cases. */
function hasCases(type: Json): boolean {
  if (!isObject(type)) return false;
  if (isObject(type.enum)) return true;
  return type.array !== undefined && hasCases(type.array);
}

export function refusePayloadEnums(app: JsonObject): void {
  const columns = payloadEnumColumns(app);
  if (columns.length === 0) return;
  const named = columns.map((c) => `"${c}"`).join(", ");
  throw new UpgradeRefusal(
    `column${columns.length === 1 ? "" : "s"} ${named} declare${columns.length === 1 ? "s" : ""} an enum whose cases carry fields, ` +
      "which Jazz no longer supports (see https://github.com/garden-co/jazz/issues/5758; no store could hold " +
      "one, as every write failed: jazz#5621). In schema.ts, change it to a string enum plus a nullable column per " +
      'field (`kind: s.enum("bug", "feature"), severity: s.int().optional(), area: s.string().optional()`), or a ' +
      "`s.json()` column when the payloads are irregular. Then run `npx jazz-tools validate` with the app's alpha.65 " +
      "(`migrations create` first, if a store holds the table) and `jazz-tools upgrade` again.",
  );
}

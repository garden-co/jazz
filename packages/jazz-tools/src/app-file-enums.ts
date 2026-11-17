/**
 * Enums in `jazz.app.json` are lists of strings (`{ "enum": ["a", "b"] }`).
 * The object form, enum cases carrying fields, is not supported (jazz#5758):
 * TS readers of an app file refuse it, naming the column.
 */

export const ENUM_CASES_ISSUE_URL = "https://github.com/garden-co/jazz/issues/5758";

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Whether a column spec (or an array's element spec) declares enum cases. */
function declaresEnumCases(spec: unknown): boolean {
  if (!isObject(spec)) return false;
  if ("enum" in spec && !Array.isArray(spec.enum)) return true;
  return declaresEnumCases(spec.type) || declaresEnumCases(spec.array);
}

/** Throw if a column of the app file's tables declares an enum whose cases carry fields. */
export function assertNoEnumCases(app: unknown): void {
  const tables = isObject(app) ? app.tables : undefined;
  if (!isObject(tables)) return;
  for (const [table, definition] of Object.entries(tables)) {
    const columns = isObject(definition) ? definition.columns : undefined;
    if (!isObject(columns)) continue;
    for (const [column, spec] of Object.entries(columns)) {
      if (declaresEnumCases(spec)) {
        throw new Error(
          `column "${table}.${column}": enum cases with fields are not supported (see ${ENUM_CASES_ISSUE_URL})`,
        );
      }
    }
  }
}

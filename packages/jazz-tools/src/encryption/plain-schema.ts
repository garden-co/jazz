/**
 * The schema an app's code sees: an encrypted column has the plaintext type
 * its declaration names (a `title` the app writes as a string, though it is
 * stored as sealed bytes) and the equality token columns are not there. It
 * is used to convert the values an app writes and reads and to type its
 * queries; the schema sent to the database, and its hash, stay as stored.
 */
import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { boundApp } from "../typed-app.js";

export type PlainEntries = Record<
  string,
  {
    columns: Record<string, { type: { columnType: ColumnType; nullable: boolean } }>;
    equality?: readonly string[];
  }
>;

const cache = new WeakMap<object, WasmSchema>();

/** The declared entries of the app's current version, from its bindings. */
export function declaredEntries(schema: WasmSchema): PlainEntries | undefined {
  const bound = boundApp(schema) as
    | { schema: { current: string; encryption?: { versions?: Record<string, PlainEntries> } } }
    | undefined;
  return bound?.schema.encryption?.versions?.[bound.schema.current];
}

/** `schema` as the app sees it; `schema` itself when the app declares no encryption. */
export function plainSchema(schema: WasmSchema, declared?: PlainEntries): WasmSchema {
  const entries = declared ?? declaredEntries(schema);
  if (!entries || Object.keys(entries).length === 0) return schema;
  const cached = cache.get(schema);
  if (cached) return cached;
  const plain: WasmSchema = { ...schema };
  for (const [table, entry] of Object.entries(entries)) {
    const stored = schema[table];
    if (!stored) continue;
    const equalityTokens = new Set(entry.equality?.map((name) => `__e2ee_eq_${name}`));
    plain[table] = {
      ...stored,
      columns: stored.columns
        .filter((column) => !equalityTokens.has(column.name))
        .map((column) => {
          const declaredColumn = entry.columns[column.name];
          return declaredColumn
            ? {
                ...column,
                column_type: declaredColumn.type.columnType,
                nullable: declaredColumn.type.nullable,
              }
            : column;
        }),
    };
  }
  cache.set(schema, plain);
  return plain;
}

/**
 * The schema an app's code sees: an encrypted column has the plaintext type
 * its declaration names (a `title` the app writes as a string, though it is
 * stored as sealed bytes) and the equality token columns are not there. It
 * is used to convert the values an app writes and reads and to type its
 * queries; the schema sent to the database, and its hash, stay as stored.
 *
 * A backend holds no keys: its reads return rows of an encrypted table
 * without their encrypted columns. Its schema ({@link plainSchema} with
 * `keyless`) has those columns optional, so nothing it decodes or converts
 * counts on a value being there.
 */
import { EQUALITY_PREFIX, hasSystemTables } from "./table-names.js";
import type { ColumnType, WasmSchema } from "../drivers/types.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { boundApp } from "../typed-app.js";

interface Declared {
  schema: {
    current: string;
    encryption?: { versions?: Record<string, object> };
    schemas?: { hash: string; schema: { tables?: Record<string, unknown> } }[];
  };
}

/**
 * Whether the app is an encrypted app: its current version declares
 * encryption, or keeps Jazz's system tables without declaring an encrypted
 * column (once added, the tables stay).
 */
export function declaresEncryption(app: SchemaSourceInput): boolean {
  const bound = boundApp(resolveSchemaSource(app)) as Declared | undefined;
  if (!bound) return false;
  const current = bound.schema.encryption?.versions?.[bound.schema.current];
  if (current !== undefined && Object.keys(current).length > 0) return true;
  const version = bound.schema.schemas?.find((entry) => entry.hash === bound.schema.current);
  return hasSystemTables(version?.schema.tables ?? {});
}

export type PlainEntries = Record<
  string,
  {
    columns: Record<string, { type: { columnType: ColumnType; nullable: boolean } }>;
    equality?: readonly string[];
  }
>;

/** Per stored schema: per declarations (or {@link DERIVED}), per keyless. */
const cache = new WeakMap<object, WeakMap<object, [WasmSchema?, WasmSchema?]>>();
/** The cache key of the declarations read from the schema's own bindings. */
const DERIVED = {};

/** The declared entries of the app's current version, from its bindings. */
export function declaredEntries(schema: WasmSchema): PlainEntries | undefined {
  const bound = boundApp(schema) as
    | { schema: { current: string; encryption?: { versions?: Record<string, PlainEntries> } } }
    | undefined;
  return bound?.schema.encryption?.versions?.[bound.schema.current];
}

/**
 * `schema` as the app sees it; `schema` itself when the app declares no
 * encryption. `keyless`: as a backend sees it, with each encrypted column
 * optional (its reads never carry one).
 */
export function plainSchema(
  schema: WasmSchema,
  declared?: PlainEntries,
  options: { keyless?: boolean } = {},
): WasmSchema {
  const entries = declared ?? declaredEntries(schema);
  if (!entries || Object.keys(entries).length === 0) return schema;
  const keyless = options.keyless === true;
  let bySchema = cache.get(schema);
  if (!bySchema) cache.set(schema, (bySchema = new WeakMap()));
  let cached = bySchema.get(declared ?? DERIVED);
  if (!cached) bySchema.set(declared ?? DERIVED, (cached = [undefined, undefined]));
  const hit = cached[keyless ? 1 : 0];
  if (hit) return hit;
  const plain: WasmSchema = { ...schema };
  for (const [table, entry] of Object.entries(entries)) {
    const stored = schema[table];
    if (!stored) continue;
    const equalityTokens = new Set(entry.equality?.map((name) => `${EQUALITY_PREFIX}${name}`));
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
                nullable: keyless || declaredColumn.type.nullable,
              }
            : column;
        }),
    };
  }
  cached[keyless ? 1 : 0] = plain;
  return plain;
}

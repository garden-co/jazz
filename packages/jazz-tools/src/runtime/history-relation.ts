import type { ColumnDescriptor, WasmSchema } from "../drivers/types.js";

export const HISTORY_PREFIX = "history.";

// Query descriptors only. Never add these virtual sources to the schema sent
// for catalogue admission, migration, permissions, or writes.
const metadataColumns: ColumnDescriptor[] = [
  {
    name: "$transactionId",
    column_type: { type: "Text" },
    nullable: false,
  },
  { name: "$seq", column_type: { type: "BigInt" }, nullable: false },
];

const cache = new WeakMap<WasmSchema, Map<string, WasmSchema[string]>>();

/** Resolve declared and virtual query sources without changing the catalogue. */
export function resolveQueryTable(
  schema: WasmSchema,
  name: string,
): WasmSchema[string] | undefined {
  if (!name.startsWith(HISTORY_PREFIX)) return schema[name];
  const base = name.slice(HISTORY_PREFIX.length);
  if (base.startsWith(HISTORY_PREFIX) || !Object.hasOwn(schema, base)) return undefined;
  let relations = cache.get(schema);
  if (!relations) cache.set(schema, (relations = new Map()));
  let relation = relations.get(name);
  if (!relation) {
    const source = schema[base]!;
    relation = {
      columns: [...source.columns, ...metadataColumns],
      ...(source.branchBy ? { branchBy: source.branchBy } : {}),
    };
    relations.set(name, relation);
  }
  return relation;
}

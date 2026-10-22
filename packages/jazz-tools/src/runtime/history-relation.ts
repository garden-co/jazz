import type { ColumnDescriptor, WasmSchema } from "../drivers/types.js";

const HISTORY_PREFIX = "history.";

/** Namespace recognition only; source validity is checked during resolution. */
export function isHistoryTable(name: string): boolean {
  return historySourceTable(name) !== undefined;
}

export function historySourceTable(name: string): string | undefined {
  return name.startsWith(HISTORY_PREFIX) ? name.slice(HISTORY_PREFIX.length) : undefined;
}

export function historyTableName(source: string): string {
  return `${HISTORY_PREFIX}${source}`;
}

// Query descriptors only. Never add these virtual sources to the schema sent
// for catalogue admission, migration, permissions, or writes.
const metadataColumns: ColumnDescriptor[] = [
  {
    name: "$transactionId",
    column_type: { type: "Text" },
    nullable: false,
  },
  { name: "$acceptedAt", column_type: { type: "Timestamp" }, nullable: false },
];

const cache = new WeakMap<WasmSchema, Map<string, WasmSchema[string]>>();

/** Resolve declared and virtual query sources without changing the catalogue. */
export function resolveQueryTable(
  schema: WasmSchema,
  name: string,
): WasmSchema[string] | undefined {
  const base = historySourceTable(name);
  if (base === undefined) return schema[name];
  if (isHistoryTable(base) || !Object.hasOwn(schema, base)) return undefined;
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

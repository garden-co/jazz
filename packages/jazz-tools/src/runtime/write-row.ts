import type { ColumnDescriptor, Value } from "../drivers/types.js";
import { isProvenanceMagicColumn } from "../magic-columns.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "./select-projection.js";

/** The immediate insert result uses schema defaults even when staging is deferred. */
export function writeRow(
  table: string,
  id: string,
  columns: readonly ColumnDescriptor[],
  values: Record<string, Value>,
) {
  const visible = columns.filter((column) => !column.name.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX));
  const valuesByColumn = new Map<string, Value>();
  for (const column of visible)
    valuesByColumn.set(column.name, values[column.name] ?? column.default ?? { type: "Null" });
  return Object.defineProperty(
    {
      table,
      id,
      values: visible
        .filter((column) => !isProvenanceMagicColumn(column.name))
        .map((column) => valuesByColumn.get(column.name)!),
    },
    "valuesByColumn",
    { value: valuesByColumn, configurable: true },
  );
}

import type { ColumnDescriptor, InsertValues } from "../drivers/types.js";

/** Validate omitted/default/null cells before publication or one-shot source use. */
export function assertRequiredRowColumnsPresent(
  columns: readonly ColumnDescriptor[],
  row: InsertValues,
  table?: string,
): void {
  for (const column of columns) {
    const value = row[column.name] ?? column.default;
    if (value && value.type !== "Null") continue;
    if (column.nullable) continue;
    throw new Error(
      table
        ? `encoding error: missing required field \`${column.name}\` on table \`${table}\``
        : `missing required column ${column.name}`,
    );
  }
}

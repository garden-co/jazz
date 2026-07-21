/**
 * Output column descriptors for aggregate query results.
 *
 * Aggregate results are synthetic rows whose fields are the aggregate aliases
 * (plus the optional group column), so subscription wire encoding/decoding
 * cannot use the source table's schema columns. Both the worker-side adapter
 * and the main-thread subscription decode derive the same descriptors from
 * the aggregate spec via this helper.
 *
 * Result types mirror the core's aggregate typing: count is an unsigned
 * 64-bit integer, avg is a double, and sum/min/max take the input column's
 * type.
 */

import type { ColumnDescriptor, ColumnType } from "../drivers/types.js";

export type AggregateColumnSpec = {
  aggregates: ReadonlyArray<{ function: string; column?: string; alias: string }>;
  groupBy?: string;
};

export function aggregateOutputColumns(
  aggregate: AggregateColumnSpec,
  tableColumns: readonly ColumnDescriptor[],
): ColumnDescriptor[] {
  const columns: ColumnDescriptor[] = [];
  if (aggregate.groupBy) {
    const group = tableColumns.find((column) => column.name === aggregate.groupBy);
    if (group) columns.push(group);
  }
  for (const entry of aggregate.aggregates) {
    columns.push({
      name: entry.alias,
      column_type: aggregateResultColumnType(entry, tableColumns),
      nullable: entry.function !== "count",
    });
  }
  return columns;
}

function aggregateResultColumnType(
  entry: { function: string; column?: string },
  tableColumns: readonly ColumnDescriptor[],
): ColumnType {
  switch (entry.function) {
    case "count":
      return { type: "BigInt" };
    case "avg":
      return { type: "Double" };
    default: {
      const source = entry.column
        ? tableColumns.find((column) => column.name === entry.column)
        : undefined;
      return source?.column_type ?? { type: "BigInt" };
    }
  }
}

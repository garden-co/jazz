import type { ColumnType } from "./drivers/types.js";

export const RESERVED_MAGIC_COLUMN_PREFIX = "$";

const REJECTED_PERMISSION_INTROSPECTION_COLUMNS = ["$canRead"] as const;
export const PROVENANCE_MAGIC_COLUMNS = [
  "$createdBy",
  "$createdAt",
  "$updatedBy",
  "$updatedAt",
] as const;
export const PROVENANCE_MAGIC_TIMESTAMP_COLUMNS = ["$createdAt", "$updatedAt"] as const;

type RejectedPermissionIntrospectionColumn =
  (typeof REJECTED_PERMISSION_INTROSPECTION_COLUMNS)[number];
export type ProvenanceMagicColumn = (typeof PROVENANCE_MAGIC_COLUMNS)[number];
export type ProvenanceMagicTimestampColumn = (typeof PROVENANCE_MAGIC_TIMESTAMP_COLUMNS)[number];

export function isPermissionIntrospectionColumn(
  column: string,
): column is RejectedPermissionIntrospectionColumn {
  return REJECTED_PERMISSION_INTROSPECTION_COLUMNS.includes(
    column as RejectedPermissionIntrospectionColumn,
  );
}

export function isProvenanceMagicColumn(column: string): column is ProvenanceMagicColumn {
  return PROVENANCE_MAGIC_COLUMNS.includes(column as ProvenanceMagicColumn);
}

export function isProvenanceMagicTimestampColumn(
  column: string,
): column is ProvenanceMagicTimestampColumn {
  return PROVENANCE_MAGIC_TIMESTAMP_COLUMNS.includes(column as ProvenanceMagicTimestampColumn);
}

export function isReservedMagicColumnName(column: string): boolean {
  return column.startsWith(RESERVED_MAGIC_COLUMN_PREFIX);
}

export function assertUserColumnNameAllowed(column: string): void {
  if (isReservedMagicColumnName(column)) {
    throw new Error(
      `Column name "${column}" is reserved for magic columns. Names starting with "${RESERVED_MAGIC_COLUMN_PREFIX}" are reserved for system fields.`,
    );
  }
}

export function magicColumnType(column: string): ColumnType | undefined {
  if (column === "$createdBy" || column === "$updatedBy") {
    return { type: "Text" };
  }
  if (column === "$createdAt" || column === "$updatedAt") {
    return { type: "Timestamp" };
  }
  return undefined;
}

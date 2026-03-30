import type { ColumnType } from "./drivers/types.js";

export const RESERVED_MAGIC_COLUMN_PREFIX = "$";

export const PERMISSION_INTROSPECTION_COLUMNS = ["$canRead", "$canEdit", "$canDelete"] as const;

export type PermissionIntrospectionColumn = (typeof PERMISSION_INTROSPECTION_COLUMNS)[number];

const MAGIC_COLUMN_SET = new Set<string>(PERMISSION_INTROSPECTION_COLUMNS);

export function isPermissionIntrospectionColumn(
  column: string,
): column is PermissionIntrospectionColumn {
  return MAGIC_COLUMN_SET.has(column);
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
  return isPermissionIntrospectionColumn(column) ? { type: "Boolean" } : undefined;
}

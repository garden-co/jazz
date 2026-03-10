import type { ColumnType } from "./drivers/types.js";

export const PERMISSION_INTROSPECTION_COLUMNS = ["$canRead", "$canEdit", "$canDelete"] as const;

export type PermissionIntrospectionColumn = (typeof PERMISSION_INTROSPECTION_COLUMNS)[number];

export const PERMISSION_INTROSPECTION_TS_TYPE = "boolean | null";

const MAGIC_COLUMN_SET = new Set<string>(PERMISSION_INTROSPECTION_COLUMNS);

export function isPermissionIntrospectionColumn(
  column: string,
): column is PermissionIntrospectionColumn {
  return MAGIC_COLUMN_SET.has(column);
}

export function magicColumnType(column: string): ColumnType | undefined {
  return isPermissionIntrospectionColumn(column) ? { type: "Boolean" } : undefined;
}

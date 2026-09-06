import type { ColumnType } from "./drivers/types.js";

export const RESERVED_MAGIC_COLUMN_PREFIX = "$";

/** Reserved provenance account; never an account that a user can register or link. */
export const SYSTEM_ACCOUNT_ID = "00000000-0000-0000-0000-000000000000";
/** Reserved provenance issuer for trusted system writes. */
export const SYSTEM_ISSUER = "urn:jazz:system";

/** Stable account ownership together with the exact identity used to write. */
export type RowAuthor = Readonly<{
  account: string;
  identity: Readonly<{ issuer: string; subject: string }>;
}>;

export function authorColumnType(): ColumnType {
  return {
    type: "Row",
    columns: [
      { name: "account", column_type: { type: "Uuid" }, nullable: false },
      {
        name: "identity",
        nullable: false,
        column_type: {
          type: "Row",
          columns: [
            { name: "issuer", column_type: { type: "Text" }, nullable: false },
            { name: "subject", column_type: { type: "Text" }, nullable: false },
          ],
        },
      },
    ],
  };
}

const REJECTED_PERMISSION_INTROSPECTION_COLUMNS = ["$canRead"] as const;
export const PROVENANCE_MAGIC_COLUMNS = [
  "$createdBy",
  "$createdAt",
  "$updatedBy",
  "$updatedAt",
  "$createdBy.account",
  "$createdBy.identity",
  "$createdBy.identity.issuer",
  "$createdBy.identity.subject",
  "$updatedBy.account",
  "$updatedBy.identity",
  "$updatedBy.identity.issuer",
  "$updatedBy.identity.subject",
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
    return authorColumnType();
  }
  if (column === "$createdAt" || column === "$updatedAt") {
    return { type: "Timestamp" };
  }
  const [root, ...path] = column.split(".");
  if ((root === "$createdBy" || root === "$updatedBy") && path.length) {
    let type = authorColumnType();
    for (const name of path) {
      if (type.type !== "Row") return undefined;
      const field = type.columns.find((field) => field.name === name);
      if (!field) return undefined;
      type = field.column_type;
    }
    return type;
  }
  return undefined;
}

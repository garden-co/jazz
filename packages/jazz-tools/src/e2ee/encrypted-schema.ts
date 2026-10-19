import type { WasmSchema } from "../drivers/types.js";
import type { Table } from "../schema.js";
import {
  currentGroupMembersTable,
  currentSpaceMembersTable,
  permissionSpaceColumn,
} from "./current-membership-names.js";

export type EncryptionDeclaration = Readonly<{
  space: string;
  columns: readonly string[];
  indexes?: Readonly<Record<string, "equality">>;
}>;

export type EncryptedSchema = Readonly<{
  logical: WasmSchema;
  scopes: ReadonlySet<string>;
  tables: ReadonlyMap<string, EncryptionDeclaration & { scope: string }>;
  explicitMembershipScopes: ReadonlySet<string>;
}>;

// Declarations belong to a schema, never to an account or a device. This also
// keeps query clones and app slices on the same logical/physical mapping.
export const encryptedSchemas = new WeakMap<WasmSchema, EncryptedSchema>();

// SDK preparation metadata only; deployed policies remain the authority.
const encryptionPolicyTables = new WeakMap<WasmSchema, Set<string>>();
const encryptionPolicyScopes = new WeakMap<WasmSchema, Set<string>>();
const spaceAdministrationScopes = new WeakMap<WasmSchema, Set<string>>();
export function registerSpaceAdministrationScopes(schema: WasmSchema, scopes: ReadonlySet<string>) {
  const registered = spaceAdministrationScopes.get(schema) ?? new Set<string>();
  for (const scope of scopes) registered.add(scope);
  spaceAdministrationScopes.set(schema, registered);
}
export function usesSpaceAdministration(schema: WasmSchema, scope: string): boolean {
  return spaceAdministrationScopes.get(schema)?.has(scope) ?? false;
}
export function registerEncryptionPolicyTables(
  schema: WasmSchema,
  tables: ReadonlySet<string>,
): void {
  let registered = encryptionPolicyTables.get(schema);
  if (!registered) encryptionPolicyTables.set(schema, (registered = new Set()));
  let scopes = encryptionPolicyScopes.get(schema);
  if (!scopes) encryptionPolicyScopes.set(schema, (scopes = new Set()));
  for (const table of tables) {
    registered.add(table);
    const scope = encryptedSchemas.get(schema)?.tables.get(table)?.scope;
    if (scope) scopes.add(scope);
  }
}
export function usesCurrentMembership(schema: WasmSchema, scope?: string): boolean {
  const metadata = encryptedSchemas.get(schema);
  if (!metadata)
    return Boolean(schema[scope ? currentSpaceMembersTable(scope) : currentGroupMembersTable]);
  const explicit = metadata.explicitMembershipScopes;
  const configured = encryptionPolicyScopes.get(schema);
  return scope
    ? explicit.has(scope) || (configured?.has(scope) ?? false)
    : explicit.size > 0 || (configured?.size ?? 0) > 0;
}
export function assertEncryptionPoliciesRegistered(schema: WasmSchema): void {
  if (encryptionPolicyTables.has(schema)) return;
  const optedIn = (encryptedSchemas.get(schema)?.explicitMembershipScopes.size ?? 0) > 0;
  if (optedIn)
    throw new Error(
      "Encryption-space policies are not registered. Import your permissions module before creating the Db, using the same application schema.",
    );
}
export function usesEncryptionSpacePolicy(schema: WasmSchema, table: string): boolean {
  assertEncryptionPoliciesRegistered(schema);
  return encryptionPolicyTables.get(schema)?.has(table) ?? false;
}

// A projected encrypted row still needs its space identity for decryption.
// Keep this dependency out of the caller's selected result properties.
export const encryptedRowSpaces = new WeakMap<object, string>();

export function equalityIndexColumn(column: string): string {
  return `__e2ee_eq_${column}`;
}

export function encryptedTableToPhysical(
  table: Table,
  declaration?: EncryptionDeclaration,
  permissionSpace?: string,
): Table {
  if (!declaration) return table;
  if (permissionSpace && table.columns.some((column) => column.name === permissionSpaceColumn))
    throw new Error(
      `Reserved encryption policy column "${permissionSpaceColumn}" in table "${table.name}"`,
    );
  return {
    ...table,
    columns: [
      ...table.columns.map((column) =>
        declaration.columns.includes(column.name)
          ? { name: column.name, sqlType: "BYTEA" as const, nullable: false }
          : column,
      ),
      ...Object.keys(declaration.indexes ?? {}).map((name) => ({
        name: equalityIndexColumn(name),
        sqlType: "BYTEA" as const,
        nullable: false,
      })),
      ...(permissionSpace
        ? [
            {
              name: permissionSpaceColumn,
              sqlType: "UUID" as const,
              nullable: false,
              references: permissionSpace,
            },
          ]
        : []),
    ],
  };
}

/**
 * Contains utilities for deploying schemas, permissions, and migrations to a Jazz server.
 */

import type { ColumnType as WasmColumnType, WasmSchema } from "../drivers/types.js";
import type { DefinedMigration } from "../migrations.js";
import { schemaDefinitionToAst } from "../migrations.js";
import { toValue } from "../runtime/value-converter.js";
import type { Lens, SqlType } from "../schema.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { collectMissingExplicitPolicyDiagnostics } from "../schema-permissions.js";
import { schemaToWasm } from "../codegen/schema-reader.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { computeSchemaHash } from "../schema-hash.js";
import {
  encodePublishedMigrationValue,
  fetchPermissionsHead,
  fetchSchemaConnectivity,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  publishStoredMigration,
  publishStoredPermissions,
  publishStoredSchema,
  type PublishedTableLens,
  type StoredPermissionsHead,
} from "../runtime/schema-fetch.js";
import {
  columnTypeSignature,
  normalizeSchemaHashInput,
  shortSchemaHash,
  wasmSchemasEqual,
} from "./schema-utils.js";

export { computeSchemaHash, shortSchemaHash };
export type { SchemaSourceInput };

export interface CatalogueServerOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface PushSchemaOptions extends CatalogueServerOptions {
  schema: SchemaSourceInput;
}

export interface PushSchemaResult {
  hash: string;
  schemaFile?: string;
  status: "published";
  objectId?: string;
}

export type DeploySchemaResult =
  | PushSchemaResult
  | {
      hash: string;
      schemaFile?: string;
      status: "already-stored";
    };

export interface PushPermissionsOptions extends CatalogueServerOptions {
  schemaHash: string;
  permissions: CompiledPermissionsMap;
}

export interface PushPermissionsResult {
  schemaHash: string;
  permissionsFile?: string;
  previousHead: StoredPermissionsHead | null;
  head: StoredPermissionsHead | null;
}

export interface PushMigrationOptions extends CatalogueServerOptions {
  fromHash: string;
  toHash: string;
  knownSchemaHashes?: readonly string[];
  migration?: DefinedMigration;
  fromSchema?: SchemaSourceInput;
  toSchema?: SchemaSourceInput;
}

export interface PushMigrationResult {
  fromHash: string;
  toHash: string;
  status: "published";
  objectId?: string;
}

export type DeployMigrationResult =
  | PushMigrationResult
  | { status: "already-connected"; fromHash: string; toHash: string }
  | { status: "missing"; fromHash: string; toHash: string };

export interface DeployOptions extends CatalogueServerOptions {
  /**
   * Current schema. Will only be published if not already stored on the server.
   */
  schema: SchemaSourceInput;
  /**
   * Permissions to publish. Omitting this param restricts `deploy` to only publish the schema.
   */
  permissions?: CompiledPermissionsMap;
  /**
   * Migration between the current server schema and the new schema.
   * Only published if there's no existing migration between these schemas.
   * In order to publish migrations, provide {@link permissions} as well.
   */
  migration?: DefinedMigration;
  /**
   * Set to `true` to publish permissions even if a migration is missing between
   * the current server schema and the new schema.
   */
  noVerify?: boolean;
}

export interface DeployResult {
  schema: DeploySchemaResult;
  migration?: DeployMigrationResult;
  permissions?: PushPermissionsResult;
  warnings: string[];
}

export class MissingMigrationError extends Error {
  readonly name = "MissingMigrationError";

  constructor(
    readonly fromHash: string,
    readonly toHash: string,
  ) {
    super(
      `Schema transition ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)} requires a migration.`,
    );
  }
}

export class MigrationHashMismatchError extends Error {
  readonly name = "MigrationHashMismatchError";

  constructor(
    readonly actualFromHash: string,
    readonly actualToHash: string,
    readonly expectedFromHash: string,
    readonly expectedToHash: string,
  ) {
    super(
      `Migration exports ${actualFromHash} -> ${actualToHash}, expected ${shortSchemaHash(expectedFromHash)} -> ${shortSchemaHash(expectedToHash)}.`,
    );
  }
}

interface ResolvedSchemaInput {
  hash: string;
  schema: WasmSchema;
}

function collectWarning(warnings: string[], message: string): void {
  warnings.push(message);
}

function resolveMigrationDefinitionWasmSchema(input: unknown): WasmSchema {
  return schemaToWasm(schemaDefinitionToAst(input as any));
}

function hashMatchesFullSchema(hash: string, fullHash: string): boolean {
  return fullHash.startsWith(normalizeSchemaHashInput(hash, "schema hash"));
}

export function resolveKnownSchemaHash(
  hash: string,
  label: string,
  knownHashes: readonly string[],
): string {
  const normalized = normalizeSchemaHashInput(hash, label);

  if (normalized.length === 64) {
    if (!knownHashes.includes(normalized)) {
      throw new Error(`No stored schema found for ${label} ${normalized}.`);
    }
    return normalized;
  }

  const matches = knownHashes.filter((candidate) => candidate.startsWith(normalized));
  if (matches.length === 0) {
    throw new Error(`No stored schema found for ${label} prefix ${normalized}.`);
  }
  if (matches.length > 1) {
    throw new Error(
      `${label} prefix ${normalized} is ambiguous: ${matches
        .map((candidate) => shortSchemaHash(candidate))
        .join(", ")}`,
    );
  }
  return matches[0]!;
}

function tableSchemasRequireRowTransform(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return true;
  }

  const leftColumnNames = left.columns.map((column) => column.name).sort();
  const rightColumnNames = right.columns.map((column) => column.name).sort();

  if (leftColumnNames.length !== rightColumnNames.length) {
    return true;
  }

  for (const [index, columnName] of leftColumnNames.entries()) {
    if (columnName !== rightColumnNames[index]) {
      return true;
    }
  }

  const leftColumns = new Map(left.columns.map((column) => [column.name, column]));
  const rightColumns = new Map(right.columns.map((column) => [column.name, column]));

  return leftColumnNames.some((columnName) => {
    const leftColumn = leftColumns.get(columnName)!;
    const rightColumn = rightColumns.get(columnName)!;
    return (
      leftColumn.nullable !== rightColumn.nullable ||
      leftColumn.references !== rightColumn.references ||
      columnTypeSignature(leftColumn.column_type) !== columnTypeSignature(rightColumn.column_type)
    );
  });
}

export function schemaTransitionRequiresRowTransform(
  fromSchema: WasmSchema,
  toSchema: WasmSchema,
): boolean {
  const fromTableNames = Object.keys(fromSchema).sort();
  const toTableNames = Object.keys(toSchema).sort();

  if (fromTableNames.length !== toTableNames.length) {
    return true;
  }

  for (const [index, tableName] of fromTableNames.entries()) {
    if (tableName !== toTableNames[index]) {
      return true;
    }
  }

  return fromTableNames.some((tableName) =>
    tableSchemasRequireRowTransform(fromSchema[tableName], toSchema[tableName]),
  );
}

export async function resolveStoredStructuralSchemaHash(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string | null> {
  const { hashes } = await fetchSchemaHashes(serverUrl, { appId, adminSecret });
  const storedSchemas = await Promise.all(
    hashes.map(async (hash) => ({
      hash,
      schema: (await fetchStoredWasmSchema(serverUrl, { appId, adminSecret, schemaHash: hash }))
        .schema,
    })),
  );

  const match = storedSchemas.find(({ schema }) => wasmSchemasEqual(schema, wasmSchema));
  return match?.hash ?? null;
}

export async function resolveStoredStructuralSchemaHashOrThrow(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string> {
  const hash = await resolveStoredStructuralSchemaHash(appId, serverUrl, adminSecret, wasmSchema);
  if (!hash) {
    throw new Error(
      "No stored structural schema matches the provided schema. Publish the structural schema before pushing permissions.",
    );
  }

  return hash;
}

function sqlTypeToWasmColumnType(sqlType: SqlType): WasmColumnType {
  if (typeof sqlType === "string") {
    switch (sqlType) {
      case "TEXT":
        return { type: "Text" };
      case "BOOLEAN":
        return { type: "Boolean" };
      case "INTEGER":
        return { type: "Integer" };
      case "REAL":
        return { type: "Double" };
      case "TIMESTAMP":
        return { type: "Timestamp" };
      case "UUID":
        return { type: "Uuid" };
      case "BYTEA":
        return { type: "Bytea" };
    }
  }

  if (sqlType.kind === "ENUM") {
    return {
      type: "Enum",
      variants: [...sqlType.variants],
    };
  }

  if (sqlType.kind === "JSON") {
    return {
      type: "Json",
      schema: sqlType.schema,
    };
  }

  return {
    type: "Array",
    element: sqlTypeToWasmColumnType(sqlType.element),
  };
}

function serializeForwardLenses(forward: readonly Lens[]): PublishedTableLens[] {
  return forward.map((tableLens) => ({
    table: tableLens.table,
    added: tableLens.added,
    removed: tableLens.removed,
    renamedFrom: tableLens.renamedFrom,
    operations: tableLens.operations.map((op) => {
      if (op.type === "rename") {
        return op;
      }

      const columnType = sqlTypeToWasmColumnType(op.sqlType);
      const value = encodePublishedMigrationValue(toValue(op.value, columnType));

      return {
        type: op.type,
        column: op.column,
        column_type: columnType,
        value,
      };
    }),
  }));
}

function isDefinedMigration(value: unknown): value is DefinedMigration {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.fromHash === "string" &&
    typeof candidate.toHash === "string" &&
    typeof candidate.from === "object" &&
    candidate.from !== null &&
    typeof candidate.to === "object" &&
    candidate.to !== null &&
    Array.isArray(candidate.forward)
  );
}

async function resolveMigrationSchema(
  serverUrl: string,
  options: CatalogueServerOptions,
  hash: string,
  schemaInput: SchemaSourceInput | undefined,
): Promise<ResolvedSchemaInput> {
  if (schemaInput) {
    return {
      hash,
      schema: resolveSchemaSource(schemaInput),
    };
  }

  const storedSchema = await fetchStoredWasmSchema(serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: hash,
  });
  return {
    hash,
    schema: storedSchema.schema,
  };
}

/**
 * Publishes a schema to the Jazz server.
 *
 * When using this function, permissions and migrations need to be updated
 * separately, using {@link pushPermissions} and {@link pushMigration}.
 *
 * Prefer using {@link deploy}, which handles all operations.
 */
export async function pushSchema(options: PushSchemaOptions): Promise<PushSchemaResult> {
  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: resolveSchemaSource(options.schema),
  });

  return {
    hash: result.hash,
    status: "published",
    objectId: result.objectId,
  };
}

/**
 * Publishes permissions to a known schema.
 *
 * The target schema must already be identified by `options.schemaHash`.
 *
 * @param options - Server, admin credentials, permissions, and schema hash for the permissions push.
 * @returns The previous and new permissions heads.
 */
export async function pushPermissions(
  options: PushPermissionsOptions,
): Promise<PushPermissionsResult> {
  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: options.schemaHash,
    permissions: options.permissions,
    expectedParentBundleObjectId: previousHead?.bundleObjectId ?? null,
  });

  return {
    schemaHash: options.schemaHash,
    previousHead,
    head,
  };
}

/**
 * Publishes the migration that connects two schemas.
 *
 * When a reviewed migration is not present, this publishes an empty migration
 * only if the schema transition does not require row transformations.
 */
export async function pushMigration(options: PushMigrationOptions): Promise<PushMigrationResult> {
  const hashes =
    options.knownSchemaHashes ??
    (
      await fetchSchemaHashes(options.serverUrl, {
        appId: options.appId,
        adminSecret: options.adminSecret,
      })
    ).hashes;
  const fromHash = resolveKnownSchemaHash(options.fromHash, "fromHash", hashes);
  const toHash = resolveKnownSchemaHash(options.toHash, "toHash", hashes);

  if (!options.migration) {
    const fromSchema = await resolveMigrationSchema(
      options.serverUrl,
      options,
      fromHash,
      options.fromSchema,
    );
    const toSchema = await resolveMigrationSchema(
      options.serverUrl,
      options,
      toHash,
      options.toSchema,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new MissingMigrationError(fromHash, toHash);
    }

    const published = await publishStoredMigration(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      fromHash,
      toHash,
      forward: [],
    });

    return {
      fromHash,
      toHash,
      status: "published",
      objectId: published.objectId,
    };
  }

  const migration = options.migration;
  if (!isDefinedMigration(migration)) {
    throw new Error("Invalid migration. Pass a value returned by defineMigration(...).");
  }

  if (
    !hashMatchesFullSchema(migration.fromHash, fromHash) ||
    !hashMatchesFullSchema(migration.toHash, toHash)
  ) {
    throw new MigrationHashMismatchError(migration.fromHash, migration.toHash, fromHash, toHash);
  }

  schemaDefinitionToAst(migration.from as any);
  schemaDefinitionToAst(migration.to as any);

  if (migration.forward.length === 0) {
    const fromSchema = resolveMigrationDefinitionWasmSchema(migration.from);
    const toSchema = resolveMigrationDefinitionWasmSchema(migration.to);

    if (schemaTransitionRequiresRowTransform(fromSchema, toSchema)) {
      throw new Error("Migration has no steps. Fill in migrate before push.");
    }
  }

  const forward = migration.forward.length === 0 ? [] : serializeForwardLenses(migration.forward);
  const published = await publishStoredMigration(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    fromHash,
    toHash,
    forward,
  });

  return {
    fromHash,
    toHash,
    status: "published",
    objectId: published.objectId,
  };
}

/**
 * Publishes a schema and optional permissions.
 *
 * When updating permissions to target a new schema, also attempts to publish a migration
 * between the old and new schemas. When a required migration is missing, returns
 * `migration.status === "missing"` without publishing permissions. Set `noVerify` to
 * publish permissions anyway.
 */
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  const wasmSchema = resolveSchemaSource(options.schema);

  const warnings: string[] = [];
  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    Object.keys(wasmSchema),
    options.permissions ?? undefined,
  )) {
    collectWarning(warnings, diagnostic.message);
  }

  const storedSchemaHash = await resolveStoredStructuralSchemaHash(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    wasmSchema,
  );

  let schema: DeploySchemaResult;
  if (storedSchemaHash) {
    schema = {
      hash: storedSchemaHash,
      status: "already-stored",
    };
  } else {
    const publishedSchema = await publishStoredSchema(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schema: wasmSchema,
    });
    schema = {
      hash: publishedSchema.hash,
      status: "published",
      objectId: publishedSchema.objectId,
    };
  }

  if (!options.permissions) {
    return { schema, warnings };
  }

  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  let migration: DeployResult["migration"];
  if (previousHead && previousHead.schemaHash !== schema.hash) {
    const { connected } = await fetchSchemaConnectivity(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      fromHash: previousHead.schemaHash,
      toHash: schema.hash,
    });

    if (connected) {
      migration = {
        status: "already-connected",
        fromHash: previousHead.schemaHash,
        toHash: schema.hash,
      };
    } else {
      try {
        migration = await pushMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          fromHash: previousHead.schemaHash,
          toHash: schema.hash,
          migration: options.migration,
          toSchema: wasmSchema,
        });
      } catch (error) {
        if (!(error instanceof MissingMigrationError)) {
          throw error;
        }

        migration = {
          status: "missing",
          fromHash: error.fromHash,
          toHash: error.toHash,
        };
        if (!options.noVerify) {
          return {
            schema,
            migration,
            warnings,
          };
        }
      }
    }
  }

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: schema.hash,
    permissions: options.permissions,
    expectedParentBundleObjectId: previousHead?.bundleObjectId ?? null,
  });

  return {
    schema,
    migration,
    permissions: {
      schemaHash: schema.hash,
      previousHead,
      head,
    },
    warnings,
  };
}

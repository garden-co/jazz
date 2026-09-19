/**
 * Contains utilities for deploying schemas, permissions, and migrations to a Jazz server.
 */

import type { ColumnType as WasmColumnType, WasmSchema } from "../drivers/types.js";
import type { DefinedMigration } from "../migrations.js";
import { schemaDefinitionToAst } from "../migrations.js";
import { toValue } from "../runtime/value-converter.js";
import type { Lens, SqlType } from "../schema.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import {
  collectMissingExplicitPolicyDiagnostics,
  mergePermissionsIntoWasmSchema,
  validatePermissionsAgainstSchema,
} from "../schema-permissions.js";
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
  tableSchemasEqual,
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

export type PushMigrationOptions = CatalogueServerOptions &
  (
    | {
        migration: DefinedMigration;
        fromHash?: string;
        toHash?: string;
      }
    | { fromHash: string; toHash: string; migration?: undefined }
  );
export interface PushMigrationResult {
  fromHash: string;
  toHash: string;
  status: "published";
  objectId?: string;
}

export type DeployMigrationResult =
  | PushMigrationResult
  | { status: "already-connected"; fromHash: string; toHash: string };

export interface DeployOptions extends CatalogueServerOptions {
  /**
   * Current schema. Will only be published if not already stored on the server.
   */
  schema: SchemaSourceInput;
  /**
   * Permissions to publish. Pass an explicit empty bundle to deny all access.
   */
  permissions: CompiledPermissionsMap;
  /**
   * Migration between the current server schema and the new schema.
   * Only published if there's no existing migration between these schemas.
   */
  migration?: DefinedMigration;
}

export interface DeployResult {
  schema: DeploySchemaResult;
  migration?: DeployMigrationResult;
  permissions: PushPermissionsResult;
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

function collectWarning(warnings: string[], message: string): void {
  warnings.push(message);
}

function resolveMigrationDefinitionWasmSchema(input: unknown): WasmSchema {
  return schemaToWasm(schemaDefinitionToAst(input as any));
}

export interface CanonicalMigrationBundle {
  fromHash: string;
  toHash: string;
  fromSchema: WasmSchema;
  toSchema: WasmSchema;
}

export function assertMigrationMatchesCanonicalBundle(
  migration: DefinedMigration,
  canonical: CanonicalMigrationBundle,
): void {
  const fromWitness = resolveMigrationDefinitionWasmSchema(migration.from);
  const toWitness = resolveMigrationDefinitionWasmSchema(migration.to);
  const endpoints = [
    {
      label: "from",
      embeddedHash: migration.fromHash,
      canonicalHash: canonical.fromHash,
      witness: fromWitness,
      schema: canonical.fromSchema,
    },
    {
      label: "to",
      embeddedHash: migration.toHash,
      canonicalHash: canonical.toHash,
      witness: toWitness,
      schema: canonical.toSchema,
    },
  ] as const;

  for (const endpoint of endpoints) {
    if (endpoint.embeddedHash !== undefined) {
      const embeddedHash = normalizeSchemaHashInput(
        endpoint.embeddedHash,
        `migration embedded ${endpoint.label}Hash`,
      );
      if (!endpoint.canonicalHash.startsWith(embeddedHash)) {
        throw new Error(
          `Migration embedded ${endpoint.label}Hash ${embeddedHash} does not match canonical ${endpoint.label}Hash ${endpoint.canonicalHash}.`,
        );
      }
    }

    for (const [tableName, witnessTable] of Object.entries(endpoint.witness)) {
      if (!tableSchemasEqual(witnessTable, endpoint.schema[tableName])) {
        throw new Error(
          `Migration ${endpoint.label} schema witness for table ${tableName} does not match canonical schema ${shortSchemaHash(endpoint.canonicalHash)}.`,
        );
      }
    }
  }

  if (
    migration.forward.length === 0 &&
    schemaTransitionRequiresRowTransform(canonical.fromSchema, canonical.toSchema)
  ) {
    throw new MissingMigrationError(canonical.fromHash, canonical.toHash);
  }
  serializeForwardLenses(migration.forward);

  for (const lens of migration.forward) {
    const sourceTable = lens.renamedFrom ?? lens.table;
    if (!lens.added && !Object.hasOwn(fromWitness, sourceTable)) {
      throw new Error(`Migration from schema witness is missing transformed table ${sourceTable}.`);
    }
    if (!lens.removed && !Object.hasOwn(toWitness, lens.table)) {
      throw new Error(`Migration to schema witness is missing transformed table ${lens.table}.`);
    }
  }
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
      case "BIGINT":
        return { type: "BigInt" };
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
    if (!sqlType.variants) {
      throw new Error("Payload enum schema lowering is not implemented yet.");
    }
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

async function loadSchema(options: CatalogueServerOptions, hash: string): Promise<WasmSchema> {
  const storedSchema = await fetchStoredWasmSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: hash,
  });
  return storedSchema.schema;
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
 * When a migration is not present, this publishes an empty migration
 * only if the schema transition does not require row transformations.
 */
export async function pushMigration(options: PushMigrationOptions): Promise<PushMigrationResult> {
  const serverOptions: CatalogueServerOptions = {
    appId: options.appId,
    serverUrl: options.serverUrl,
    adminSecret: options.adminSecret,
  };
  const migration = options.migration;
  const fromWitness = migration ? resolveMigrationDefinitionWasmSchema(migration.from) : undefined;
  const toWitness = migration ? resolveMigrationDefinitionWasmSchema(migration.to) : undefined;
  const fromHash = options.fromHash ?? (await computeSchemaHash(fromWitness!));
  const toHash = options.toHash ?? (await computeSchemaHash(toWitness!));
  const fromSchema = options.fromHash ? await loadSchema(serverOptions, fromHash) : fromWitness!;
  const toSchema = options.toHash ? await loadSchema(serverOptions, toHash) : toWitness!;

  if (migration) {
    assertMigrationMatchesCanonicalBundle(migration, {
      fromHash,
      toHash,
      fromSchema,
      toSchema,
    });
  }

  const forward = serializeForwardLenses(migration?.forward ?? []);
  if (forward.length === 0 && schemaTransitionRequiresRowTransform(fromSchema, toSchema)) {
    throw new MissingMigrationError(fromHash, toHash);
  }

  const published = await publishStoredMigration(serverOptions.serverUrl, {
    appId: serverOptions.appId,
    adminSecret: serverOptions.adminSecret,
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

/** Publishes the schema, required migration, and permissions */
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  if (options.permissions == null) {
    throw new Error("deploy requires an explicit permissions bundle. Pass {} to deny all access.");
  }
  if ("noVerify" in options) {
    throw new Error("noVerify is no longer supported; deploy requires a complete migration path.");
  }
  const wasmSchema = mergePermissionsIntoWasmSchema(resolveSchemaSource(options.schema), {});
  validatePermissionsAgainstSchema(Object.keys(wasmSchema), options.permissions);
  const warnings: string[] = [];
  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    Object.keys(wasmSchema),
    options.permissions,
  )) {
    collectWarning(warnings, diagnostic.message);
  }
  const storedSchemaHash = await resolveStoredStructuralSchemaHash(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    wasmSchema,
  );
  const targetHash = storedSchemaHash ?? (await computeSchemaHash(wasmSchema));
  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });
  let connected = false;
  const transitioning = previousHead && previousHead.schemaHash !== targetHash;
  if (transitioning) {
    if (storedSchemaHash) {
      ({ connected } = await fetchSchemaConnectivity(options.serverUrl, {
        appId: options.appId,
        adminSecret: options.adminSecret,
        fromHash: previousHead.schemaHash,
        toHash: targetHash,
      }));
    }
    if (!connected) {
      const fromSchema = await loadSchema(options, previousHead.schemaHash);
      if (options.migration) {
        assertMigrationMatchesCanonicalBundle(options.migration, {
          fromHash: previousHead.schemaHash,
          toHash: targetHash,
          fromSchema,
          toSchema: wasmSchema,
        });
      }
      if (
        (!options.migration || options.migration.forward.length === 0) &&
        schemaTransitionRequiresRowTransform(fromSchema, wasmSchema)
      ) {
        throw new MissingMigrationError(previousHead.schemaHash, targetHash);
      }
    }
  }

  // All local inputs are checked before publication. Network failures can still
  // leave stored artifacts; retrying deploy reuses them before advancing the head.
  const schema: DeploySchemaResult = storedSchemaHash
    ? { hash: storedSchemaHash, status: "already-stored" }
    : {
        ...(await publishStoredSchema(options.serverUrl, {
          appId: options.appId,
          adminSecret: options.adminSecret,
          schema: wasmSchema,
        })),
        status: "published",
      };
  let migration: DeployResult["migration"];
  if (transitioning) {
    migration = connected
      ? { status: "already-connected", fromHash: previousHead.schemaHash, toHash: schema.hash }
      : await pushMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          fromHash: previousHead.schemaHash,
          toHash: schema.hash,
          ...(options.migration ? { migration: options.migration } : {}),
        });
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
    permissions: { schemaHash: schema.hash, previousHead, head },
    warnings,
  };
}

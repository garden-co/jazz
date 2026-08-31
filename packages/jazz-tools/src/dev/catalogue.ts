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
  legacyByteaStructuralSchemaHash,
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
  | { status: "already-connected"; fromHash: string; toHash: string }
  | { status: "missing"; fromHash: string; toHash: string };

export interface DeployOptions extends CatalogueServerOptions {
  /**
   * Current schema. Will only be published if not already stored on the server.
   */
  schema: SchemaSourceInput;
  /**
   * Permissions to publish. Omitting this param restricts `deploy` to publishing
   * the schema and any required identity compatibility edge.
   */
  permissions?: CompiledPermissionsMap;
  /**
   * Migration between the current permissions head schema and the new schema.
   * Only published if there's no existing migration between these schemas.
   * In order to publish this permissions-driven migration, provide
   * {@link permissions} as well.
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

export type StoredStructuralSchemaIdentity = {
  hash: string;
  format: "current" | "legacy-bytea" | "historical";
};

/**
 * Resolves the durable catalogue identity that owns a stored schema payload.
 *
 * The current hash is preferred. The historical Bytea/Double-collision hash is
 * then resolved explicitly, without treating it as an alias for the current
 * identity. Callers that publish a release must publish the current identity
 * and connect a legacy result with a normal catalogue migration edge.
 */
export async function resolveStoredStructuralSchemaIdentity(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<StoredStructuralSchemaIdentity | null> {
  const { hashes } = await fetchSchemaHashes(serverUrl, { appId, adminSecret });
  const currentHash = await computeSchemaHash(wasmSchema);
  const legacyByteaHash = legacyByteaStructuralSchemaHash(wasmSchema);
  const candidates: StoredStructuralSchemaIdentity[] = [];

  if (hashes.includes(currentHash)) {
    candidates.push({ hash: currentHash, format: "current" });
  }
  if (legacyByteaHash !== currentHash && hashes.includes(legacyByteaHash)) {
    candidates.push({ hash: legacyByteaHash, format: "legacy-bytea" });
  }
  for (const hash of hashes) {
    if (hash !== currentHash && hash !== legacyByteaHash) {
      candidates.push({ hash, format: "historical" });
    }
  }

  for (const candidate of candidates) {
    const stored = await fetchStoredWasmSchema(serverUrl, {
      appId,
      adminSecret,
      schemaHash: candidate.hash,
    });
    if (wasmSchemasEqual(stored.schema, wasmSchema)) {
      return candidate;
    }
  }

  return null;
}

export async function resolveStoredStructuralSchemaHash(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string | null> {
  const identity = await resolveStoredStructuralSchemaIdentity(
    appId,
    serverUrl,
    adminSecret,
    wasmSchema,
  );
  return identity?.hash ?? null;
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

/**
 * Publishes a schema, required identity compatibility edges, and optional permissions.
 *
 * A durable legacy Bytea identity is connected to the corrected identity even
 * when permissions are not part of the deploy. When updating permissions to
 * target a new schema, also attempts to publish a migration between the old and
 * new schemas. When a required migration is missing, returns
 * `migration.status === "missing"` without publishing permissions. Set `noVerify`
 * to publish permissions anyway.
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

  const storedIdentity = await resolveStoredStructuralSchemaIdentity(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    wasmSchema,
  );
  const storedSchemaHash =
    storedIdentity && storedIdentity.format !== "legacy-bytea" ? storedIdentity.hash : null;

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

  let migration: DeployResult["migration"];
  if (storedIdentity?.format === "legacy-bytea" && storedIdentity.hash !== schema.hash) {
    const { connected } = await fetchSchemaConnectivity(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      fromHash: storedIdentity.hash,
      toHash: schema.hash,
    });

    migration = connected
      ? {
          status: "already-connected",
          fromHash: storedIdentity.hash,
          toHash: schema.hash,
        }
      : await pushMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          fromHash: storedIdentity.hash,
          toHash: schema.hash,
        });
  }

  if (!options.permissions) {
    return { schema, migration, warnings };
  }

  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  if (
    previousHead &&
    previousHead.schemaHash !== schema.hash &&
    (migration?.fromHash !== previousHead.schemaHash || migration?.toHash !== schema.hash)
  ) {
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
        migration = await pushMigration(
          options.migration
            ? {
                appId: options.appId,
                serverUrl: options.serverUrl,
                adminSecret: options.adminSecret,
                migration: options.migration,
                fromHash: previousHead.schemaHash,
                toHash: schema.hash,
              }
            : {
                appId: options.appId,
                serverUrl: options.serverUrl,
                adminSecret: options.adminSecret,
                fromHash: previousHead.schemaHash,
                toHash: schema.hash,
              },
        );
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

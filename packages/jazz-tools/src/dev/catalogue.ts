import { validateSchemaAndPermissions } from "./schema-validation.js";
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
} from "../schema-permissions.js";
import { schemaToWasm } from "../codegen/schema-reader.js";
import { resolveSchemaSource, type SchemaSourceInput } from "../schema-source.js";
import { computeSchemaHash } from "../schema-hash.js";
import {
  encodePublishedMigrationValue,
  fetchStoredWasmSchema,
  publishDeployment,
  type DeploymentArtifacts,
  type DeploymentResponse,
  type PublishedTableLens,
} from "./catalogue-api.js";
import {
  columnTypeSignature,
  normalizeSchemaHashInput,
  shortSchemaHash,
  tableSchemasEqual,
} from "./schema-utils.js";

import { fetchMigrationGraph, type MigrationGraph } from "./migration-graph.js";

export { computeSchemaHash, shortSchemaHash };
export type { SchemaSourceInput };

export interface CatalogueServerOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

export interface DeploySchemaResult {
  hash: string;
  schemaFile?: string;
  status: "published" | "already-stored";
}

export interface DeployOptions extends CatalogueServerOptions {
  /**
   * Current schema. Will only be published if not already stored on the server.
   */
  schema: SchemaSourceInput;
  /**
   * Permissions to publish. Pass an explicit empty bundle to deny all access.
   */
  permissions: CompiledPermissionsMap;
  /** Historical schemas to include in the deployment. */
  schemas?: readonly SchemaSourceInput[];
  /** All reviewed migrations, including converging branches. */
  migrations?: readonly DefinedMigration[];
  /** A single reviewed migration. May also be supplied alongside migrations. */
  migration?: DefinedMigration;
}

export interface DeployResult extends DeploymentResponse {
  schema: DeploySchemaResult;
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

export function serializeForwardLenses(forward: readonly Lens[]): PublishedTableLens[] {
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

/** Diff against the fetched graph and publish all missing artifacts in one request. */
export async function deployArtifacts(
  options: CatalogueServerOptions,
  graph: MigrationGraph,
  artifacts: DeploymentArtifacts,
): Promise<DeployResult> {
  const storedSchemas = new Set(graph.schemas);
  const storedMigrations = new Set(
    graph.migrations.map((edge) => `${edge.fromHash}:${edge.toHash}`),
  );
  const result = await publishDeployment(options.serverUrl, {
    ...options,
    ...artifacts,
    schemas: artifacts.schemas.filter(({ hash }) => !storedSchemas.has(hash)),
    migrations: artifacts.migrations.filter(
      (edge) => !storedMigrations.has(`${edge.fromHash}:${edge.toHash}`),
    ),
  });
  const target = artifacts.schemas.find(({ hash }) => hash === artifacts.targetSchemaHash)!;
  return {
    ...result,
    schema: {
      hash: artifacts.targetSchemaHash,
      status: result.published.schemas.includes(artifacts.targetSchemaHash)
        ? "published"
        : "already-stored",
    },
    warnings: collectMissingExplicitPolicyDiagnostics(
      Object.keys(target.schema),
      artifacts.permissions,
    ).map((diagnostic) => diagnostic.message),
  };
}

/** Publish schemas, migrations and permissions together. */
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  if (options.permissions == null) {
    throw new Error("deploy requires an explicit permissions bundle. Pass {} to deny all access.");
  }
  if ("noVerify" in options) {
    throw new Error("noVerify is no longer supported; deploy requires a complete migration path.");
  }
  const schema = mergePermissionsIntoWasmSchema(resolveSchemaSource(options.schema), {});
  await validateSchemaAndPermissions(schema, options.permissions);
  const targetSchemaHash = await computeSchemaHash(schema);
  const graph = await fetchMigrationGraph(options);
  const schemas = new Map<string, WasmSchema>();
  for (const source of [...(options.schemas ?? []), schema]) {
    const structural = mergePermissionsIntoWasmSchema(resolveSchemaSource(source), {});
    schemas.set(await computeSchemaHash(structural), structural);
  }
  const definitions = [
    ...(options.migrations ?? []),
    ...(options.migration ? [options.migration] : []),
  ];
  // Migration witnesses can omit unchanged tables. With explicit hashes, use
  // canonical local schemas or fetch the server's version before checking them.
  const resolveEndpoint = async (witness: unknown, embedded: string | undefined) => {
    if (!embedded) {
      const schema = mergePermissionsIntoWasmSchema(
        resolveMigrationDefinitionWasmSchema(witness),
        {},
      );
      const hash = await computeSchemaHash(schema);
      if (!schemas.has(hash)) schemas.set(hash, schema);
      return { hash, schema: schemas.get(hash)! };
    }
    const hash = resolveKnownSchemaHash(embedded, "migration schema hash", [
      ...new Set([...graph.schemas, ...schemas.keys()]),
    ]);
    return { hash, schema: schemas.get(hash) ?? (await loadSchema(options, hash)) };
  };
  const migrations: DeploymentArtifacts["migrations"] = [];
  for (const migration of definitions) {
    const from = await resolveEndpoint(migration.from, migration.fromHash);
    const to = await resolveEndpoint(migration.to, migration.toHash);
    assertMigrationMatchesCanonicalBundle(migration, {
      fromHash: from.hash,
      toHash: to.hash,
      fromSchema: from.schema,
      toSchema: to.schema,
    });
    migrations.push({
      fromHash: from.hash,
      toHash: to.hash,
      forward: serializeForwardLenses(migration.forward),
    });
  }
  return deployArtifacts(options, graph, {
    targetSchemaHash,
    schemas: [...schemas].map(([hash, schema]) => ({ hash, schema })),
    migrations,
    permissions: options.permissions,
  });
}

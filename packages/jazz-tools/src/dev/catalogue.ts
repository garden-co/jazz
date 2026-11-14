import { validateSchemaAndPermissions } from "./schema-validation.js";
/**
 * Contains utilities for deploying schemas, permissions, and migrations to a Jazz server.
 */

import type { ColumnType as WasmColumnType, WasmSchema } from "../drivers/types.js";
import type { DefinedMigration } from "../migrations.js";
import { schemaDefinitionToAst } from "../migrations.js";
import { toValue } from "../runtime/value-converter.js";
import {
  isAppPermissions,
  shippedArtifact,
  SHIP_ARTIFACT_HINT,
  SHIP_PERMISSIONS_HINT,
  type AppPermissions,
} from "../runtime/app-artifact.js";
import type { Lens, SqlType } from "../schema.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import {
  collectMissingExplicitPolicyDiagnostics,
  mergePermissionsIntoWasmSchema,
} from "../schema-permissions.js";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
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
  /**
   * Confirms switching tables of the active schema to latest-only history,
   * which drops their past versions for good.
   */
  allowDroppingHistory?: boolean;
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
    if (lens.added && Object.hasOwn(canonical.fromSchema, sourceTable)) {
      throw new Error(
        `Migration added table ${sourceTable} already exists in the canonical source schema.`,
      );
    }
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
  options: CatalogueServerOptions & { allowDroppingHistory?: boolean },
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

/**
 * Publish an app as its code declares it. An app that ships its
 * `jazz.app.json` (`defineApp(schema, { artifact })`) publishes exactly the
 * deployment Jazz's Rust reader takes from that file and its
 * `jazz.permissions.json` (`permissions`): every schema version, migration
 * and the permissions the backend's file holds. The schema `defineApp` built
 * must be the file's current version, or the file is out of date and
 * refused. An app on `{ migrations: "none" }` publishes its schema alone,
 * with the permissions `definePermissions` made. This is what `createJazz`
 * publishes on start, so anything else that serves the app (a test server)
 * must publish the same, or a client opening the app with its lenses
 * disagrees with the server about its history.
 */
export async function deployApp(
  options: CatalogueServerOptions & {
    app: SchemaSourceInput;
    permissions: BackendPermissions;
    allowDroppingHistory?: boolean;
  },
): Promise<DeployResult> {
  const declared = resolveSchemaSource(options.app);
  const schema = mergePermissionsIntoWasmSchema(declared, {});
  const resolved = await resolveAppPermissions(options.app, options.permissions);
  await validateSchemaAndPermissions(schema, resolved.permissions);
  if (!resolved.deployment) {
    const targetSchemaHash = await computeSchemaHash(schema);
    const graph = await fetchMigrationGraph(options);
    return deployArtifacts(options, graph, {
      targetSchemaHash,
      schemas: [{ hash: targetSchemaHash, schema }],
      migrations: [],
      permissions: resolved.permissions,
    });
  }
  const loaded = resolved.deployment;
  const graph = await fetchMigrationGraph(options);
  const result = await deployArtifacts(options, graph, {
    targetSchemaHash: loaded.current,
    schemas: loaded.deployment.schemas.map(({ hash, schema }) => ({ hash, schema: schema.tables })),
    migrations: loaded.deployment.migrations,
    permissions: resolved.permissions,
    publishedPermissions: loaded.deployment.permissions,
  });
  // The core computes the bundle id as the files' writer did; anything else
  // would pin this backend's sessions to permissions the file doesn't hold.
  if (result.bundle && result.bundle !== loaded.bundle) {
    throw new Error(
      `The core published bundle ${result.bundle} for this app, but its jazz.permissions.json names bundle ${loaded.bundle}: is the core older than this jazz-tools?`,
    );
  }
  return result;
}

/**
 * What a backend serving an app is given as its permissions: the app's
 * `jazz.permissions.json` when the app ships its `jazz.app.json`, or the map
 * `definePermissions` makes for an app on `{ migrations: "none" }`.
 */
export type BackendPermissions = CompiledPermissionsMap | AppPermissions;

/** What Jazz's Rust reader takes from an app's two files (jazz-napi `loadAppArtifact`). */
export interface LoadedArtifact {
  current: string;
  bundle: string;
  deployment: {
    targetSchemaHash: string;
    schemas: Array<{ hash: string; schema: { tables: WasmSchema } }>;
    migrations: Array<{ fromHash: string; toHash: string; forward: PublishedTableLens[] }>;
    permissions: Record<string, unknown>;
  };
}

/** Why `permissions` can't serve `app`, if it can't: checked when `createJazz` is called. */
export function backendPermissionsMismatch(
  app: SchemaSourceInput,
  permissions: BackendPermissions,
): string | null {
  const artifact = shippedArtifact(resolveSchemaSource(app));
  if (artifact && !isAppPermissions(permissions)) {
    return (
      "an app that ships its jazz.app.json takes its permissions from jazz.permissions.json, not from definePermissions(...). " +
      SHIP_PERMISSIONS_HINT
    );
  }
  if (!artifact && isAppPermissions(permissions)) {
    return (
      "jazz.permissions.json is for an app that ships its jazz.app.json, but this app's defineApp was given none. " +
      SHIP_ARTIFACT_HINT
    );
  }
  return null;
}

const resolvedPermissions = new WeakMap<
  object,
  WeakMap<object, Promise<{ permissions: CompiledPermissionsMap; deployment?: LoadedArtifact }>>
>();

/**
 * The permissions a backend serving `app` enforces, as `definePermissions`
 * makes them, and for an app that ships its `jazz.app.json`, the deployment
 * Jazz's Rust reader takes from it and `permissions` (its
 * `jazz.permissions.json`). Refuses permissions that can't serve the app
 * ({@link backendPermissionsMismatch}), and a file that is out of date with
 * schema.ts. The same app and permissions always give the same result.
 */
export function resolveAppPermissions(
  app: SchemaSourceInput,
  permissions: BackendPermissions,
): Promise<{ permissions: CompiledPermissionsMap; deployment?: LoadedArtifact }> {
  const schema = resolveSchemaSource(app);
  let forSchema = resolvedPermissions.get(schema);
  if (!forSchema) resolvedPermissions.set(schema, (forSchema = new WeakMap()));
  let resolved = forSchema.get(permissions);
  if (!resolved) {
    resolved = resolveUncached(schema, permissions);
    forSchema.set(permissions, resolved);
  }
  return resolved;
}

async function resolveUncached(
  declared: WasmSchema,
  permissions: BackendPermissions,
): Promise<{ permissions: CompiledPermissionsMap; deployment?: LoadedArtifact }> {
  const mismatch = backendPermissionsMismatch(declared, permissions);
  if (mismatch) throw new Error(`createJazz: ${mismatch}`);
  const artifact = shippedArtifact(declared);
  if (!artifact || !isAppPermissions(permissions)) {
    return { permissions: permissions as CompiledPermissionsMap };
  }
  const napi = await import("jazz-napi");
  const loaded = napi.loadAppArtifact(
    JSON.stringify(artifact),
    JSON.stringify(permissions),
  ) as unknown as LoadedArtifact;
  const hash = napi.schemaHash(
    JSON.stringify(mergePermissionsIntoWasmSchema(declared, {}), runtimeSchemaJsonReplacer),
  );
  if (hash !== loaded.current) {
    throw new Error(
      `jazz.app.json is out of date with schema.ts: the app's schema is ${hash}, but the file's current schema is ${loaded.current}. ` +
        "Run `npx jazz-tools validate` and commit jazz.app.json and jazz.permissions.json.",
    );
  }
  return { permissions: compiledFromEngine(loaded.deployment.permissions), deployment: loaded };
}

/**
 * The engine's JSON for a permissions bundle as `definePermissions` makes
 * it: the same, except that the engine writes an absent `using` or
 * `with_check` as `null`, which is dropped. Nothing deeper changes: a `null`
 * inside a policy is a literal.
 */
function compiledFromEngine(permissions: Record<string, unknown>): CompiledPermissionsMap {
  const operation = (policy: unknown) =>
    Object.fromEntries(
      Object.entries((policy ?? {}) as Record<string, unknown>).filter(([, expr]) => expr !== null),
    );
  return Object.fromEntries(
    Object.entries(permissions).map(([table, policies]) => [
      table,
      Object.fromEntries(
        Object.entries(policies as Record<string, unknown>).map(([name, policy]) => [
          name,
          operation(policy),
        ]),
      ),
    ]),
  ) as CompiledPermissionsMap;
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
  // Explicit hashes select canonical schemas. A single direct deployment also
  // accepts partial witnesses against the latest bundle's schema and requested target.
  // Graph migrations retain their own endpoints; never infer them from the head.
  const direct = options.migration !== undefined && !options.migrations?.length;
  const resolveEndpoint = async (
    label: "from" | "to",
    witness: unknown,
    embedded: string | undefined,
    canonicalHash?: string | null,
  ) => {
    if (embedded) {
      const hash = resolveKnownSchemaHash(embedded, "migration schema hash", [
        ...new Set([...graph.schemas, ...schemas.keys()]),
      ]);
      return { hash, schema: schemas.get(hash) ?? (await loadSchema(options, hash)) };
    }
    const witnessSchema = mergePermissionsIntoWasmSchema(
      resolveMigrationDefinitionWasmSchema(witness),
      {},
    );
    const witnessHash = await computeSchemaHash(witnessSchema);
    if (canonicalHash && !schemas.has(witnessHash) && !graph.schemas.includes(witnessHash)) {
      // A witness without tables would match every schema, so it is no evidence.
      if (Object.keys(witnessSchema).length === 0) {
        throw new Error(
          `Migration ${label} schema witness names no tables, so it cannot identify a published schema; supply ${label}Hash.`,
        );
      }
      const canonical = schemas.get(canonicalHash) ?? (await loadSchema(options, canonicalHash));
      if (
        Object.entries(witnessSchema).every(([table, definition]) =>
          tableSchemasEqual(definition, canonical[table]),
        )
      ) {
        schemas.set(canonicalHash, canonical);
        return { hash: canonicalHash, schema: canonical };
      }
    }
    if (!schemas.has(witnessHash)) schemas.set(witnessHash, witnessSchema);
    return { hash: witnessHash, schema: schemas.get(witnessHash)! };
  };
  const migrations: DeploymentArtifacts["migrations"] = [];
  for (const migration of definitions) {
    const to = await resolveEndpoint(
      "to",
      migration.to,
      migration.toHash,
      direct ? targetSchemaHash : null,
    );
    let sourceHash = direct && to.hash === targetSchemaHash ? graph.latestBundleSchemaHash : null;
    if (direct && to.hash === targetSchemaHash && !sourceHash && !migration.fromHash) {
      const outgoing = new Set(graph.migrations.map((edge) => edge.fromHash));
      const tips = graph.schemas.filter((hash) => !outgoing.has(hash));
      if (tips.length === 1) sourceHash = tips[0]!;
    }
    let unresolvedSource = "Partial migration source is ambiguous; supply fromHash.";
    if (sourceHash === targetSchemaHash && !migration.fromHash) {
      // Re-deploying a direct migration must still validate against its original
      // source. Only an unambiguous incoming edge can supply an omitted hash.
      const incoming = graph.migrations.filter((edge) => edge.toHash === targetSchemaHash);
      sourceHash = incoming.length === 1 ? incoming[0]!.fromHash : null;
      if (incoming.length === 0) {
        unresolvedSource = `Partial migration source cannot be resolved: target schema ${shortSchemaHash(targetSchemaHash)} is already the latest published bundle's schema and no published migration leads to it; supply fromHash.`;
      }
      if (incoming.length > 1) {
        const witnessHash = await computeSchemaHash(
          mergePermissionsIntoWasmSchema(resolveMigrationDefinitionWasmSchema(migration.from), {}),
        );
        if (!schemas.has(witnessHash) && !graph.schemas.includes(witnessHash)) {
          throw new Error("Partial migration source is ambiguous; supply fromHash.");
        }
      }
    }
    if (
      direct &&
      to.hash === targetSchemaHash &&
      !sourceHash &&
      !migration.fromHash &&
      graph.schemas.length
    ) {
      const witnessHash = await computeSchemaHash(
        mergePermissionsIntoWasmSchema(resolveMigrationDefinitionWasmSchema(migration.from), {}),
      );
      if (!schemas.has(witnessHash) && !graph.schemas.includes(witnessHash))
        throw new Error(unresolvedSource);
    }
    const from = await resolveEndpoint("from", migration.from, migration.fromHash, sourceHash);
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

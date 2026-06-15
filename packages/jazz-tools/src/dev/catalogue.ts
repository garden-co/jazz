import { access, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";
import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
} from "../drivers/types.js";
import type { DefinedMigration } from "../migrations.js";
import { schemaDefinitionToAst } from "../migrations.js";
import { loadWasmModule } from "../runtime/client.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import { collectMissingExplicitPolicyDiagnostics } from "../schema-permissions.js";
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
import type { Lens, SqlType } from "../schema.js";
import { toValue } from "../runtime/value-converter.js";

export type CatalogueEvent =
  | { type: "schema-loaded"; schemaFile: string }
  | { type: "schema-published"; hash: string; objectId?: string }
  | { type: "schema-skipped"; hash: string; reason: "already-stored" }
  | { type: "permissions-loaded"; permissionsFile: string }
  | { type: "permissions-published"; schemaHash: string; version?: number }
  | { type: "permissions-skipped"; reason: "missing-permissions-file" }
  | { type: "migration-published"; fromHash: string; toHash: string; filePath?: string }
  | { type: "migration-skipped"; reason: "already-connected"; fromHash: string; toHash: string }
  | { type: "warning"; message: string };

export interface CatalogueProjectOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushSchemaOptions extends CatalogueProjectOptions {}

export interface PushSchemaResult {
  hash: string;
  schemaFile: string;
  status: "published";
  objectId?: string;
}

export type DeploySchemaResult =
  | PushSchemaResult
  | {
      hash: string;
      schemaFile: string;
      status: "already-stored";
    };

export interface PushPermissionsOptions extends CatalogueProjectOptions {
  schemaHash: string;
}

export interface PushPermissionsResult {
  schemaHash: string;
  permissionsFile: string;
  previousHead: StoredPermissionsHead | null;
  head: StoredPermissionsHead | null;
}

export interface PushSchemaCatalogueOptions extends CatalogueProjectOptions {
  env?: string;
  userBranch?: string;
  enableLogs?: boolean;
}

export interface PushMigrationOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  migrationsDir: string;
  fromHash: string;
  toHash: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushMigrationResult {
  fromHash: string;
  toHash: string;
  status: "published";
  filePath?: string;
  objectId?: string;
}

export interface DeployResult {
  schema: DeploySchemaResult;
  migration?:
    | PushMigrationResult
    | { status: "already-connected"; fromHash: string; toHash: string };
  permissions?: PushPermissionsResult;
  warnings: string[];
}

export interface DeployOptions extends CatalogueProjectOptions {
  migrationsDir: string;
  noVerify?: boolean;
}

function emit(options: { onEvent?: (event: CatalogueEvent) => void }, event: CatalogueEvent): void {
  options.onEvent?.(event);
}

function emitWarning(
  options: { onEvent?: (event: CatalogueEvent) => void },
  warnings: string[],
  message: string,
): void {
  warnings.push(message);
  emit(options, { type: "warning", message });
}

function ensurePermissionsProject(compiled: LoadedSchemaProject): LoadedSchemaProject & {
  permissions: NonNullable<LoadedSchemaProject["permissions"]>;
  permissionsFile: string;
} {
  if (!compiled.permissions || !compiled.permissionsFile) {
    throw new Error(
      "No permissions found for this app. Create a permissions.ts file before using permissions commands.",
    );
  }

  return compiled as LoadedSchemaProject & {
    permissions: NonNullable<LoadedSchemaProject["permissions"]>;
    permissionsFile: string;
  };
}

export async function pushSchema(options: PushSchemaOptions): Promise<PushSchemaResult> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  emit(options, { type: "schema-published", hash: result.hash, objectId: result.objectId });

  return {
    hash: result.hash,
    schemaFile: compiled.schemaFile,
    status: "published",
    objectId: result.objectId,
  };
}

export async function pushPermissions(
  options: PushPermissionsOptions,
): Promise<PushPermissionsResult> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });

  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: options.schemaHash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: previousHead?.bundleObjectId ?? null,
  });

  emit(options, {
    type: "permissions-published",
    schemaHash: options.schemaHash,
    version: head?.version,
  });

  return {
    schemaHash: options.schemaHash,
    permissionsFile: compiled.permissionsFile,
    previousHead,
    head,
  };
}

export async function pushSchemaCatalogue(
  options: PushSchemaCatalogueOptions,
): Promise<{ hash: string }> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  emit(options, { type: "schema-published", hash: result.hash, objectId: result.objectId });

  if (compiled.permissions) {
    const { head } = await fetchPermissionsHead(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    });
    const { head: permissionsHead } = await publishStoredPermissions(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schemaHash: result.hash,
      permissions: compiled.permissions,
      expectedParentBundleObjectId: head?.bundleObjectId ?? null,
    });
    emit(options, {
      type: "permissions-published",
      schemaHash: result.hash,
      version: permissionsHead?.version,
    });
  }

  if (options.enableLogs === true) {
    console.log(
      `[jazz-schema-push] published ${result.hash} from ${compiled.schemaFile} to ${options.serverUrl}`,
    );
  }

  return { hash: result.hash };
}

export const SHORT_SCHEMA_HASH_LENGTH = 12;

export function normalizeSchemaHashInput(hash: string, label: string): string {
  const normalized = hash.trim().toLowerCase();
  if (!/^[0-9a-f]{12,64}$/.test(normalized)) {
    throw new Error(`${label} must be a 12-64 character lowercase hex schema hash.`);
  }
  return normalized;
}

export function shortSchemaHash(hash: string): string {
  return normalizeSchemaHashInput(hash, "schema hash").slice(0, SHORT_SCHEMA_HASH_LENGTH);
}

export function hashMatchesFullSchema(hash: string, fullHash: string): boolean {
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

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

export function snapshotsDirForMigrations(migrationsDir: string): string {
  return join(migrationsDir, "snapshots");
}

export interface ResolvedSchemaInput {
  hash: string;
  schema: WasmSchema;
}

export interface SnapshotEntry {
  hash: string;
  fileName: string;
  filePath: string;
  schema: WasmSchema;
}

// Supports both millisecond and microsecond-precision timestamps.
export function looksLikeSnapshotFileName(fileName: string): boolean {
  return /^(?:\d{8,17}T\d{6}-)?[0-9a-f]{12}\.json$/i.test(fileName);
}

export async function readSnapshotEntry(
  dir: string,
  fileName: string,
): Promise<SnapshotEntry | null> {
  if (!looksLikeSnapshotFileName(fileName)) {
    return null;
  }

  const filePath = join(dir, fileName);
  const schema = JSON.parse(await readFile(filePath, "utf8")) as WasmSchema;
  return {
    hash: await computeSchemaHash(schema),
    fileName,
    filePath,
    schema,
  };
}

export async function listSnapshotEntries(dir: string): Promise<SnapshotEntry[]> {
  if (!(await pathExists(dir))) {
    return [];
  }

  const files = await readdir(dir);
  return (await Promise.all(files.map((fileName) => readSnapshotEntry(dir, fileName)))).filter(
    (entry): entry is SnapshotEntry => entry !== null,
  );
}

export async function listSnapshotEntriesForMigrations(
  migrationsDir: string,
): Promise<SnapshotEntry[]> {
  return listSnapshotEntries(snapshotsDirForMigrations(migrationsDir));
}

export function snapshotFilename(hash: string, timestamp: string = createTimestamp()): string {
  return `${timestamp}-${shortSchemaHash(hash)}.json`;
}

export function createTimestamp(now: Date = new Date()): string {
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const hours = String(now.getUTCHours()).padStart(2, "0");
  const minutes = String(now.getUTCMinutes()).padStart(2, "0");
  const seconds = String(now.getUTCSeconds()).padStart(2, "0");
  return `${year}${month}${day}T${hours}${minutes}${seconds}`;
}

export function createSnapshotTimestampFromPublishedAt(
  publishedAt: number | null | undefined,
  fallbackNow: Date = new Date(),
): string {
  if (typeof publishedAt !== "number" || !Number.isFinite(publishedAt) || publishedAt < 0) {
    return createTimestamp(fallbackNow);
  }

  return createTimestamp(new Date(publishedAt));
}

export async function writeSnapshotSchemaForMigrations(
  migrationsDir: string,
  fileName: string,
  schema: WasmSchema,
): Promise<string> {
  const dir = snapshotsDirForMigrations(migrationsDir);
  await mkdir(dir, { recursive: true });
  const filePath = join(dir, fileName);
  await writeFile(filePath, `${JSON.stringify(schema, null, 2)}\n`);
  return filePath;
}

let wasmModulePromise: Promise<any> | null = null;

async function loadCatalogueWasmModule(): Promise<any> {
  if (!wasmModulePromise) {
    wasmModulePromise = loadWasmModule();
  }
  return wasmModulePromise;
}

export async function computeSchemaHash(schema: WasmSchema): Promise<string> {
  const wasmModule = await loadCatalogueWasmModule();
  const runtime = new wasmModule.WasmRuntime(
    JSON.stringify(schema),
    "jazz-tools-cli",
    "dev",
    "main",
    null,
    null,
  );

  try {
    return runtime.getSchemaHash();
  } finally {
    if (typeof runtime.free === "function") {
      runtime.free();
    }
  }
}

export function columnTypeSignature(columnType: WasmColumnType): string {
  return JSON.stringify(columnType);
}

export function columnsEqual(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return (
    left.name === right.name &&
    left.nullable === right.nullable &&
    left.references === right.references &&
    left.merge_strategy === right.merge_strategy &&
    columnTypeSignature(left.column_type) === columnTypeSignature(right.column_type)
  );
}

export function indexedColumnsEqual(
  left: readonly string[] | undefined,
  right: readonly string[] | undefined,
): boolean {
  if (!left && !right) {
    return true;
  }
  if (!left || !right || left.length !== right.length) {
    return false;
  }

  const leftColumns = [...left].sort();
  const rightColumns = [...right].sort();
  return leftColumns.every((column, index) => column === rightColumns[index]);
}

export function tableSchemasEqual(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return false;
  }

  if (left.columns.length !== right.columns.length) {
    return false;
  }

  if (!indexedColumnsEqual(left.indexed_columns, right.indexed_columns)) {
    return false;
  }

  const leftColumns = [...left.columns].sort((a, b) => a.name.localeCompare(b.name));
  const rightColumns = [...right.columns].sort((a, b) => a.name.localeCompare(b.name));

  return leftColumns.every((column, index) => columnsEqual(column, rightColumns[index]!));
}

export function tableSchemasRequireRowTransform(
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

export function wasmSchemasEqual(left: WasmSchema, right: WasmSchema): boolean {
  const leftTableNames = Object.keys(left).sort();
  const rightTableNames = Object.keys(right).sort();

  if (leftTableNames.length !== rightTableNames.length) {
    return false;
  }

  return leftTableNames.every((tableName, index) => {
    if (tableName !== rightTableNames[index]) {
      return false;
    }
    return tableSchemasEqual(left[tableName], right[tableName]);
  });
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

export function sqlTypeToWasmColumnType(sqlType: SqlType): WasmColumnType {
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

export function isDefinedMigration(value: unknown): value is DefinedMigration {
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

let importCounter = 0;

async function bundleToTempFile(filePath: string): Promise<string> {
  const sourceDir = dirname(resolve(filePath));
  const outFile = join(sourceDir, `.jazz-bundle-${++importCounter}.mjs`);

  await build({
    entryPoints: [resolve(filePath)],
    bundle: true,
    format: "esm",
    platform: "node",
    outfile: outFile,
    packages: "external",
  });

  return outFile;
}

export async function loadDefinedMigration(filePath: string): Promise<DefinedMigration> {
  const outFile = await bundleToTempFile(filePath);
  try {
    const loaded = (await import(pathToFileURL(outFile).href)) as {
      default?: unknown;
      migration?: unknown;
    };
    const migration = unwrapMigrationExport(loaded.default ?? loaded.migration);
    if (!isDefinedMigration(migration)) {
      throw new Error(
        `Invalid migration export in ${basename(filePath)}. Export default defineMigration(...).`,
      );
    }
    return migration;
  } finally {
    await rm(outFile, { force: true }).catch(() => undefined);
  }
}

export function unwrapMigrationExport(value: unknown): unknown {
  let current = value;

  while (
    typeof current === "object" &&
    current !== null &&
    "default" in current &&
    Object.keys(current as Record<string, unknown>).length === 1
  ) {
    current = (current as { default: unknown }).default;
  }

  return current;
}

export async function findMigrationFile(
  migrationsDir: string,
  fromHash: string,
  toHash: string,
): Promise<string> {
  if (!(await pathExists(migrationsDir))) {
    throw new Error(`No migration file found in ${migrationsDir} for ${fromHash} -> ${toHash}.`);
  }

  const fromShortHash = shortSchemaHash(fromHash);
  const toShortHash = shortSchemaHash(toHash);
  const files = await readdir(migrationsDir);
  const matches = files
    .filter((file) => file.endsWith(".ts"))
    .filter(
      (file) =>
        file.includes(`-${fromShortHash}-${toShortHash}.ts`) ||
        file.includes(`-${fromHash}-${toHash}.ts`),
    );

  if (matches.length === 0) {
    throw new Error(`No migration file found in ${migrationsDir} for ${fromHash} -> ${toHash}.`);
  }

  if (matches.length > 1) {
    throw new Error(
      `Multiple migration files found for ${fromHash} -> ${toHash}: ${matches.join(", ")}`,
    );
  }

  return join(migrationsDir, matches[0]!);
}

export async function resolveSnapshotEntry(
  dir: string,
  hash: string,
  label: string,
): Promise<SnapshotEntry | null> {
  const entries = await listSnapshotEntries(dir);
  if (entries.length === 0) {
    return null;
  }

  const normalized = normalizeSchemaHashInput(hash, label);
  if (normalized.length === 64) {
    return entries.find((entry) => entry.hash === normalized) ?? null;
  }

  const matches = entries.filter((entry) => entry.hash.startsWith(normalized));
  if (matches.length === 0) {
    return null;
  }
  if (matches.length > 1) {
    throw new Error(
      `${label} prefix ${normalized} is ambiguous: ${matches
        .map((entry) => shortSchemaHash(entry.hash))
        .join(", ")}`,
    );
  }
  return matches[0]!;
}

export async function resolveLocalHistoricalSchema(
  migrationsDir: string,
  hash: string,
  label: string,
): Promise<ResolvedSchemaInput | null> {
  const localEntry = await resolveSnapshotEntry(
    snapshotsDirForMigrations(migrationsDir),
    hash,
    label,
  );
  if (!localEntry) {
    return null;
  }

  return {
    hash: localEntry.hash,
    schema: localEntry.schema,
  };
}

export async function resolveRemoteHistoricalSchema(
  migrationsDir: string,
  hash: string,
  label: string,
  appId: string,
  serverUrl: string,
  adminSecret: string,
): Promise<ResolvedSchemaInput> {
  const normalized = normalizeSchemaHashInput(hash, label);
  const resolvedHash =
    normalized.length === 64
      ? normalized
      : resolveKnownSchemaHash(
          normalized,
          label,
          (await fetchSchemaHashes(serverUrl, { appId, adminSecret })).hashes,
        );

  try {
    const storedSchema = await fetchStoredWasmSchema(serverUrl, {
      appId,
      adminSecret,
      schemaHash: resolvedHash,
    });
    await writeSnapshotSchemaForMigrations(
      migrationsDir,
      snapshotFilename(
        resolvedHash,
        createSnapshotTimestampFromPublishedAt(storedSchema.publishedAt),
      ),
      storedSchema.schema,
    );
    return { hash: resolvedHash, schema: storedSchema.schema };
  } catch (error) {
    if (error instanceof Error && /Schema fetch failed: 404/i.test(error.message)) {
      throw new Error(`No stored schema found for ${label} ${resolvedHash}.`);
    }
    throw error;
  }
}

export async function resolveHistoricalSchema(
  migrationsDir: string,
  hash: string,
  label: string,
  appId: string,
  serverUrl: string,
  adminSecret: string,
): Promise<ResolvedSchemaInput> {
  return (
    (await resolveLocalHistoricalSchema(migrationsDir, hash, label)) ??
    resolveRemoteHistoricalSchema(migrationsDir, hash, label, appId, serverUrl, adminSecret)
  );
}

export async function pushMigration(options: PushMigrationOptions): Promise<PushMigrationResult> {
  const { hashes } = await fetchSchemaHashes(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });
  const fromHash = resolveKnownSchemaHash(options.fromHash, "fromHash", hashes);
  const toHash = resolveKnownSchemaHash(options.toHash, "toHash", hashes);
  let filePath: string | null = null;

  try {
    filePath = await findMigrationFile(options.migrationsDir, fromHash, toHash);
  } catch (error) {
    if (
      !(error instanceof Error) ||
      !error.message.startsWith(`No migration file found in ${options.migrationsDir}`)
    ) {
      throw error;
    }
  }

  if (!filePath) {
    const fromSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      fromHash,
      "fromHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );
    const toSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      toHash,
      "toHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new Error(
        `No migration file found in ${options.migrationsDir} for ${fromHash} -> ${toHash}. Run \`jazz-tools migrations create ${options.appId} --fromHash ${shortSchemaHash(fromHash)} --toHash ${shortSchemaHash(toHash)}\` first.`,
      );
    }

    const published = await publishStoredMigration(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      fromHash,
      toHash,
      forward: [],
    });

    emit(options, { type: "migration-published", fromHash, toHash });
    return {
      fromHash,
      toHash,
      status: "published",
      objectId: published.objectId,
    };
  }

  const migration = await loadDefinedMigration(filePath);

  if (
    !hashMatchesFullSchema(migration.fromHash, fromHash) ||
    !hashMatchesFullSchema(migration.toHash, toHash)
  ) {
    throw new Error(
      `Migration ${basename(filePath)} exports ${migration.fromHash} -> ${migration.toHash}, expected ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)}.`,
    );
  }

  schemaDefinitionToAst(migration.from as any);
  schemaDefinitionToAst(migration.to as any);

  if (migration.forward.length === 0) {
    const fromSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      fromHash,
      "fromHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );
    const toSchema = await resolveHistoricalSchema(
      options.migrationsDir,
      toHash,
      "toHash",
      options.appId,
      options.serverUrl,
      options.adminSecret,
    );

    if (schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
      throw new Error(`Migration ${basename(filePath)} has no steps. Fill in migrate before push.`);
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

  emit(options, { type: "migration-published", fromHash, toHash, filePath });
  return {
    fromHash,
    toHash,
    status: "published",
    filePath,
    objectId: published.objectId,
  };
}

function disconnectedSchemaMessage(
  appId: string,
  migrationsDir: string,
  fromHash: string,
  toHash: string,
): string {
  const fromShortHash = shortSchemaHash(fromHash);
  const toShortHash = shortSchemaHash(toHash);
  return `The new permissions schema ${toShortHash} is not connected to the previous permissions schema ${fromShortHash} on the server. Reads and writes may fail until you push a migration. Run \`jazz-tools migrations create ${appId} --fromHash ${fromShortHash} --toHash ${toShortHash}\` to create a migration and then re-run this command.`;
}

export async function deploy(options: DeployOptions): Promise<DeployResult> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const warnings: string[] = [];
  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    compiled.schema.tables,
    compiled.permissions,
  )) {
    emitWarning(options, warnings, diagnostic.message);
  }

  const storedSchemaHash = await resolveStoredStructuralSchemaHash(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );

  let schema: DeploySchemaResult;
  if (storedSchemaHash) {
    emit(options, {
      type: "schema-skipped",
      hash: storedSchemaHash,
      reason: "already-stored",
    });
    schema = {
      hash: storedSchemaHash,
      schemaFile: compiled.schemaFile,
      status: "already-stored",
    };
  } else {
    const publishedSchema = await publishStoredSchema(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schema: compiled.wasmSchema,
    });
    emit(options, {
      type: "schema-published",
      hash: publishedSchema.hash,
      objectId: publishedSchema.objectId,
    });
    schema = {
      hash: publishedSchema.hash,
      schemaFile: compiled.schemaFile,
      status: "published",
      objectId: publishedSchema.objectId,
    };
  }

  if (!compiled.permissions || !compiled.permissionsFile) {
    emit(options, { type: "permissions-skipped", reason: "missing-permissions-file" });
    return { schema, warnings };
  }

  emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });

  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  let migration: DeployResult["migration"];
  if (previousHead && previousHead.schemaHash !== schema.hash) {
    try {
      const { connected } = await fetchSchemaConnectivity(options.serverUrl, {
        appId: options.appId,
        adminSecret: options.adminSecret,
        fromHash: previousHead.schemaHash,
        toHash: schema.hash,
      });

      if (connected) {
        emit(options, {
          type: "migration-skipped",
          reason: "already-connected",
          fromHash: previousHead.schemaHash,
          toHash: schema.hash,
        });
        migration = {
          status: "already-connected",
          fromHash: previousHead.schemaHash,
          toHash: schema.hash,
        };
      } else {
        migration = await pushMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          migrationsDir: options.migrationsDir,
          fromHash: previousHead.schemaHash,
          toHash: schema.hash,
          onEvent: options.onEvent,
        });
      }
    } catch (error) {
      const migrationMissingPrefix = `No migration file found in ${options.migrationsDir}`;
      if (!(error instanceof Error) || !error.message.startsWith(migrationMissingPrefix)) {
        throw error;
      }

      const message = disconnectedSchemaMessage(
        options.appId,
        options.migrationsDir,
        previousHead.schemaHash,
        schema.hash,
      );
      if (!options.noVerify) {
        throw new Error(message);
      }
      emitWarning(options, warnings, message);
    }
  }

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash: schema.hash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: previousHead?.bundleObjectId ?? null,
  });

  emit(options, {
    type: "permissions-published",
    schemaHash: schema.hash,
    version: head?.version,
  });

  return {
    schema,
    migration,
    permissions: {
      schemaHash: schema.hash,
      permissionsFile: compiled.permissionsFile,
      previousHead,
      head,
    },
    warnings,
  };
}

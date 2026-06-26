import { access, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";
import type { ColumnType as WasmColumnType, WasmSchema } from "../drivers/types.js";
import type { DefinedMigration } from "../migrations.js";
import { schemaDefinitionToAst } from "../migrations.js";
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
import { renderMigrationStub } from "./migrations.js";
import {
  columnTypeSignature,
  normalizeSchemaHashInput,
  shortSchemaHash,
  structuralSchemaHash,
  wasmSchemasEqual,
} from "./schema-utils.js";

export { shortSchemaHash };

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
  /**
   * Directory where the `schema.ts` and `permissions.ts` files are located
   */
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
  /**
   * Directory containing migration files. Defaults to `<schemaDir>/migrations`.
   */
  migrationsDir?: string;
  noVerify?: boolean;
}

interface ValidateProjectOptions {
  schemaDir: string;
}

interface ValidateProjectResult {
  schemaFile: string;
  permissionsFile?: string;
  tableCount: number;
  warnings: string[];
}

interface ExportSchemaOptions {
  schemaDir: string;
  migrationsDir?: string;
  schemaHash?: string;
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
}

interface ExportSchemaResult {
  schema: WasmSchema;
  hash: string;
  snapshotPath: string | null;
}

interface CurrentSchemaHashOptions {
  schemaDir: string;
}

interface CurrentSchemaHashResult {
  schemaFile: string;
  hash: string;
}

interface CreateMigrationOptions {
  appId?: string;
  serverUrl?: string;
  adminSecret?: string;
  migrationsDir: string;
  schemaDir: string;
  fromHash?: string;
  toHash?: string;
  name?: string;
}

type CreateMigrationResult =
  | {
      status: "initial-snapshot";
      snapshotPath: string;
    }
  | {
      status: "unchanged";
    }
  | {
      status: "migration-not-required";
      fromHash: string;
      toHash: string;
      snapshotPath: string | null;
    }
  | {
      status: "generated";
      filePath: string;
      fromHash: string;
      toHash: string;
      needsRename: boolean;
      snapshotPath: string | null;
    };

interface PermissionsStatusOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
}

interface PermissionsStatusResult {
  schemaFile: string;
  permissionsFile: string;
  localSchemaHash: string;
  head: StoredPermissionsHead | null;
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

export async function validateProject(
  options: ValidateProjectOptions,
): Promise<ValidateProjectResult> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  return {
    schemaFile: compiled.schemaFile,
    permissionsFile: compiled.permissionsFile,
    tableCount: compiled.schema.tables.length,
    warnings: collectMissingExplicitPolicyDiagnostics(
      compiled.schema.tables.map((table) => table.name),
      compiled.permissions,
    ).map((diagnostic) => diagnostic.message),
  };
}

export async function exportSchema(options: ExportSchemaOptions): Promise<ExportSchemaResult> {
  if (options.schemaHash) {
    return resolveExportedSchemaByHash({ ...options, schemaHash: options.schemaHash });
  }

  const currentSchema = await loadCurrentSchema(options.schemaDir);
  return {
    ...currentSchema,
    snapshotPath: await ensureLocalSnapshot(
      options.schemaDir,
      options.migrationsDir,
      currentSchema,
    ),
  };
}

export async function getCurrentSchemaHash(
  options: CurrentSchemaHashOptions,
): Promise<CurrentSchemaHashResult> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  return {
    schemaFile: compiled.schemaFile,
    hash: await computeSchemaHash(compiled.wasmSchema),
  };
}

export async function getPermissionsStatus(
  options: PermissionsStatusOptions,
): Promise<PermissionsStatusResult> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  const localSchemaHash = await resolveStoredStructuralSchemaHashOrThrow(
    options.appId,
    options.serverUrl,
    options.adminSecret,
    compiled.wasmSchema,
  );
  const { head } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  return {
    schemaFile: compiled.schemaFile,
    permissionsFile: compiled.permissionsFile,
    localSchemaHash,
    head,
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

/**
 * Publishes permissions to a known schema.
 *
 * The target schema must already be identified by `options.schemaHash`.
 * @throws when no `permissions.ts` file exists.
 *
 * @param options - Project, server, admin credentials, and schema hash for the permissions push.
 * @returns The previous and new permissions heads.
 */
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

function hashMatchesFullSchema(hash: string, fullHash: string): boolean {
  return fullHash.startsWith(normalizeSchemaHashInput(hash, "schema hash"));
}

function resolveKnownSchemaHash(
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

function snapshotsDirForMigrations(migrationsDir: string): string {
  return join(migrationsDir, "snapshots");
}

interface ResolvedSchemaInput {
  hash: string;
  schema: WasmSchema;
}

interface SnapshotEntry {
  hash: string;
  fileName: string;
  filePath: string;
  schema: WasmSchema;
}

// Supports both millisecond and microsecond-precision timestamps.
function looksLikeSnapshotFileName(fileName: string): boolean {
  return /^(?:\d{8,17}T\d{6}-)?[0-9a-f]{12}\.json$/i.test(fileName);
}

async function readSnapshotEntry(dir: string, fileName: string): Promise<SnapshotEntry | null> {
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

async function listSnapshotEntries(dir: string): Promise<SnapshotEntry[]> {
  if (!(await pathExists(dir))) {
    return [];
  }

  const files = await readdir(dir);
  return (await Promise.all(files.map((fileName) => readSnapshotEntry(dir, fileName)))).filter(
    (entry): entry is SnapshotEntry => entry !== null,
  );
}

async function listSnapshotEntriesForMigrations(migrationsDir: string): Promise<SnapshotEntry[]> {
  return listSnapshotEntries(snapshotsDirForMigrations(migrationsDir));
}

function snapshotFilename(hash: string, timestamp: string = createTimestamp()): string {
  return `${timestamp}-${shortSchemaHash(hash)}.json`;
}

function createTimestamp(now: Date = new Date()): string {
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const hours = String(now.getUTCHours()).padStart(2, "0");
  const minutes = String(now.getUTCMinutes()).padStart(2, "0");
  const seconds = String(now.getUTCSeconds()).padStart(2, "0");
  return `${year}${month}${day}T${hours}${minutes}${seconds}`;
}

function createSnapshotTimestampFromPublishedAt(
  publishedAt: number | null | undefined,
  fallbackNow: Date = new Date(),
): string {
  if (typeof publishedAt !== "number" || !Number.isFinite(publishedAt) || publishedAt < 0) {
    return createTimestamp(fallbackNow);
  }

  return createTimestamp(new Date(publishedAt));
}

async function writeSnapshotSchemaForMigrations(
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

function defaultMigrationsDir(schemaDir: string): string {
  return join(schemaDir, "migrations");
}

function resolvedMigrationsDir(schemaDir: string, migrationsDir?: string): string {
  return migrationsDir ?? defaultMigrationsDir(schemaDir);
}

function snapshotsDir(schemaDir: string, migrationsDir?: string): string {
  return snapshotsDirForMigrations(resolvedMigrationsDir(schemaDir, migrationsDir));
}

async function listLocalSnapshotEntries(
  schemaDir: string,
  migrationsDir?: string,
): Promise<SnapshotEntry[]> {
  return listSnapshotEntries(snapshotsDir(schemaDir, migrationsDir));
}

async function resolveLocalSnapshotEntry(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  label: string,
): Promise<SnapshotEntry | null> {
  return resolveSnapshotEntry(snapshotsDir(schemaDir, migrationsDir), hash, label);
}

async function loadLocalSnapshotSchema(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  label: string,
): Promise<ResolvedSchemaInput | null> {
  const entry = await resolveLocalSnapshotEntry(schemaDir, migrationsDir, hash, label);
  if (!entry) {
    return null;
  }

  return {
    hash: entry.hash,
    schema: entry.schema,
  };
}

async function writeSnapshotSchema(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  schema: WasmSchema,
  timestamp: string = createTimestamp(),
): Promise<string> {
  const dir = snapshotsDir(schemaDir, migrationsDir);
  await mkdir(dir, { recursive: true });
  const filePath = join(dir, snapshotFilename(hash, timestamp));
  await writeFile(filePath, `${JSON.stringify(schema, null, 2)}\n`);
  return filePath;
}

async function ensureLocalSnapshot(
  schemaDir: string,
  migrationsDir: string | undefined,
  schema: ResolvedSchemaInput,
): Promise<string | null> {
  const entries = await listLocalSnapshotEntries(schemaDir, migrationsDir);
  if (entries.some((entry) => entry.hash === schema.hash)) {
    return null;
  }

  return writeSnapshotSchema(schemaDir, migrationsDir, schema.hash, schema.schema);
}

function requireServerValue(value: string | undefined, kind: "serverUrl" | "adminSecret"): string {
  if (value) {
    return value;
  }

  if (kind === "serverUrl") {
    throw new Error(
      "Missing server URL. Pass --server-url <url> or set JAZZ_SERVER_URL (or a framework-prefixed form such as VITE_JAZZ_SERVER_URL).",
    );
  }

  throw new Error("Missing admin secret. Pass --admin-secret <secret> or set JAZZ_ADMIN_SECRET.");
}

function requireAppId(appId: string | undefined): string {
  if (appId) {
    return appId;
  }

  throw new Error(
    "Missing app ID. Pass an <appId> positional argument or set JAZZ_APP_ID (or a framework-prefixed form such as VITE_JAZZ_APP_ID).",
  );
}

async function resolveExportedSchemaByHash(
  options: ExportSchemaOptions & { schemaHash: string },
): Promise<ExportSchemaResult> {
  const schemaHash = normalizeSchemaHashInput(options.schemaHash, "schema hash");
  const local = await loadLocalSnapshotSchema(
    options.schemaDir,
    options.migrationsDir,
    schemaHash,
    "schema hash",
  );
  if (local) {
    return {
      ...local,
      snapshotPath: null,
    };
  }

  const serverUrl = requireServerValue(options.serverUrl, "serverUrl");
  const adminSecret = requireServerValue(options.adminSecret, "adminSecret");
  const appId = requireAppId(options.appId);
  const resolvedHash =
    schemaHash.length === 64
      ? schemaHash
      : resolveKnownSchemaHash(
          schemaHash,
          "schema hash",
          (await fetchSchemaHashes(serverUrl, { appId, adminSecret })).hashes,
        );
  const storedSchema = await fetchStoredWasmSchema(serverUrl, {
    appId,
    adminSecret,
    schemaHash: resolvedHash,
  });
  const snapshotPath = await writeSnapshotSchema(
    options.schemaDir,
    options.migrationsDir,
    resolvedHash,
    storedSchema.schema,
    createSnapshotTimestampFromPublishedAt(storedSchema.publishedAt),
  );

  return {
    hash: resolvedHash,
    schema: storedSchema.schema,
    snapshotPath,
  };
}

async function computeSchemaHash(schema: WasmSchema): Promise<string> {
  return structuralSchemaHash(schema);
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

async function resolveStoredStructuralSchemaHash(
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

async function resolveStoredStructuralSchemaHashOrThrow(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  wasmSchema: WasmSchema,
): Promise<string> {
  const hash = await resolveStoredStructuralSchemaHash(appId, serverUrl, adminSecret, wasmSchema);
  if (!hash) {
    throw new Error(
      "No stored structural schema matches the local schema.ts. Publish the structural schema before pushing permissions.",
    );
  }

  return hash;
}

function schemaTransitionRequiresRowTransform(
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

function normalizeMigrationName(name: string): string {
  const normalized = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  if (normalized.length === 0) {
    throw new Error(
      "Migration name must contain at least one ASCII letter or digit after normalization.",
    );
  }

  return normalized;
}

function migrationFilename(
  migrationsDir: string,
  fromHash: string,
  toHash: string,
  name: string = "unnamed",
  timestamp: string = createTimestamp(),
): string {
  return join(
    migrationsDir,
    `${timestamp}-${name}-${shortSchemaHash(fromHash)}-${shortSchemaHash(toHash)}.ts`,
  );
}

function isCommittedSnapshotFileName(fileName: string): boolean {
  return /^\d{8}T\d{6}-[0-9a-f]{12}\.json$/i.test(fileName);
}

async function loadLatestCommittedSnapshot(
  migrationsDir: string,
): Promise<ResolvedSchemaInput | null> {
  const entries = await listSnapshotEntriesForMigrations(migrationsDir);
  const latest = entries
    .filter((entry) => isCommittedSnapshotFileName(entry.fileName))
    .sort((left, right) => left.fileName.localeCompare(right.fileName))
    .at(-1);
  if (!latest) {
    return null;
  }

  return {
    hash: latest.hash,
    schema: latest.schema,
  };
}

async function ensureCommittedSnapshot(
  migrationsDir: string,
  schema: ResolvedSchemaInput,
  timestamp: string,
): Promise<string | null> {
  const entries = await listSnapshotEntriesForMigrations(migrationsDir);
  if (
    entries.some(
      (entry) => entry.hash === schema.hash && isCommittedSnapshotFileName(entry.fileName),
    )
  ) {
    return null;
  }

  return writeSnapshotSchemaForMigrations(
    migrationsDir,
    snapshotFilename(schema.hash, timestamp),
    schema.schema,
  );
}

async function loadCurrentSchema(schemaDir: string): Promise<ResolvedSchemaInput> {
  const compiled = await loadCompiledSchema(schemaDir);
  return {
    hash: await computeSchemaHash(compiled.wasmSchema),
    schema: compiled.wasmSchema,
  };
}

async function resolveHistoricalSchemaForCreateMigration(
  migrationsDir: string,
  hash: string,
  label: string,
  appId: string | undefined,
  serverUrl: string | undefined,
  adminSecret: string | undefined,
): Promise<ResolvedSchemaInput> {
  const local = await resolveLocalHistoricalSchema(migrationsDir, hash, label);
  if (local) {
    return { hash: local.hash, schema: local.schema };
  }

  return resolveRemoteHistoricalSchema(
    migrationsDir,
    hash,
    label,
    requireAppId(appId),
    requireServerValue(serverUrl, "serverUrl"),
    requireServerValue(adminSecret, "adminSecret"),
  );
}

export async function createMigration(
  options: CreateMigrationOptions,
): Promise<CreateMigrationResult> {
  const explicitHashFlow = Boolean(options.fromHash || options.toHash);

  await mkdir(options.migrationsDir, { recursive: true });
  const currentSchema =
    !explicitHashFlow || !options.toHash ? await loadCurrentSchema(options.schemaDir) : null;

  let fromSchema: ResolvedSchemaInput;
  let toSchema: ResolvedSchemaInput;
  let shouldWriteCommittedSnapshot = false;
  const timestamp = createTimestamp();

  if (explicitHashFlow) {
    if (options.fromHash) {
      fromSchema = await resolveHistoricalSchemaForCreateMigration(
        options.migrationsDir,
        options.fromHash,
        "fromHash",
        options.appId,
        options.serverUrl,
        options.adminSecret,
      );
    } else {
      const latest = await loadLatestCommittedSnapshot(options.migrationsDir);
      if (!latest) {
        throw new Error(
          "No committed snapshot found. Provide --fromHash or run `jazz-tools migrations create` once to create an initial snapshot.",
        );
      }
      fromSchema = latest;
    }

    toSchema = options.toHash
      ? await resolveHistoricalSchemaForCreateMigration(
          options.migrationsDir,
          options.toHash,
          "toHash",
          options.appId,
          options.serverUrl,
          options.adminSecret,
        )
      : currentSchema!;
    shouldWriteCommittedSnapshot = !options.toHash;
  } else {
    const latest = await loadLatestCommittedSnapshot(options.migrationsDir);
    if (!latest) {
      return {
        status: "initial-snapshot",
        snapshotPath: (await ensureCommittedSnapshot(
          options.migrationsDir,
          currentSchema!,
          timestamp,
        ))!,
      };
    }

    if (latest.hash === currentSchema!.hash) {
      return { status: "unchanged" };
    }

    fromSchema = latest;
    toSchema = currentSchema!;
    shouldWriteCommittedSnapshot = true;
  }

  if (fromSchema.hash === toSchema.hash) {
    return { status: "unchanged" };
  }

  if (!schemaTransitionRequiresRowTransform(fromSchema.schema, toSchema.schema)) {
    return {
      status: "migration-not-required",
      fromHash: fromSchema.hash,
      toHash: toSchema.hash,
      snapshotPath: shouldWriteCommittedSnapshot
        ? await ensureCommittedSnapshot(options.migrationsDir, toSchema, timestamp)
        : null,
    };
  }

  const filePath = migrationFilename(
    options.migrationsDir,
    fromSchema.hash,
    toSchema.hash,
    options.name ? normalizeMigrationName(options.name) : undefined,
    timestamp,
  );
  if (await pathExists(filePath)) {
    throw new Error(`Migration stub already exists: ${filePath}`);
  }

  const stub = renderMigrationStub({
    fromHash: fromSchema.hash,
    toHash: toSchema.hash,
    fromSchema: fromSchema.schema,
    toSchema: toSchema.schema,
  });
  await writeFile(filePath, stub);

  return {
    status: "generated",
    filePath,
    fromHash: fromSchema.hash,
    toHash: toSchema.hash,
    needsRename: !options.name,
    snapshotPath: shouldWriteCommittedSnapshot
      ? await ensureCommittedSnapshot(options.migrationsDir, toSchema, timestamp)
      : null,
  };
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

async function loadDefinedMigration(filePath: string): Promise<DefinedMigration> {
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

function unwrapMigrationExport(value: unknown): unknown {
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

async function findMigrationFile(
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

async function resolveSnapshotEntry(
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

async function resolveLocalHistoricalSchema(
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

async function resolveRemoteHistoricalSchema(
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

async function resolveHistoricalSchema(
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

/**
 * Publishes the migration that connects two schemas.
 *
 * When a reviewed migration file is not present, this publishes an empty migration
 * only if the schema transition does not require row transformations.
 */
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

function disconnectedSchemaMessage(appId: string, fromHash: string, toHash: string): string {
  const fromShortHash = shortSchemaHash(fromHash);
  const toShortHash = shortSchemaHash(toHash);
  return `The new permissions schema ${toShortHash} is not connected to the previous permissions schema ${fromShortHash} on the server. Reads and writes may fail until you push a migration. Run \`jazz-tools migrations create ${appId} --fromHash ${fromShortHash} --toHash ${toShortHash}\` to create a migration and then re-run this command.`;
}

/**
 * Publishes the current schema and permissions.
 *
 * When updating a schema, also attempts to publish a migration between the old and new schemas.
 * Set `noVerify` to return a warning instead of throwing if that migration is missing.
 */
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  const migrationsDir = options.migrationsDir ?? join(options.schemaDir, "migrations");
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const warnings: string[] = [];
  for (const diagnostic of collectMissingExplicitPolicyDiagnostics(
    compiled.schema.tables.map((table) => table.name),
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
          migrationsDir,
          fromHash: previousHead.schemaHash,
          toHash: schema.hash,
          onEvent: options.onEvent,
        });
      }
    } catch (error) {
      const migrationMissingPrefix = `No migration file found in ${migrationsDir}`;
      if (!(error instanceof Error) || !error.message.startsWith(migrationMissingPrefix)) {
        throw error;
      }

      const message = disconnectedSchemaMessage(
        options.appId,
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

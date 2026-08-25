/**
 * Contains utilities for deploying schemas, permissions, and migrations to a Jazz server.
 *
 * These are internal utilities for the CLI and dev tooling, which read/write schemas,
 * permissions, and migrations from/to the FS.
 *
 * Prefer using {@link catalogue.ts} utils whenever possible.
 */

import { constants as fsConstants } from "node:fs";
import {
  access,
  lstat,
  link,
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  unlink,
  writeFile,
  type FileHandle,
} from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { basename, dirname, isAbsolute, join, parse, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";
import type { WasmSchema } from "../drivers/types.js";
import type { DefinedMigration } from "../migrations.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import { collectMissingExplicitPolicyDiagnostics } from "../schema-permissions.js";
import { collectConventionalProvenanceDiagnostics } from "../provenance-guidance.js";
import {
  fetchPermissionsHead,
  fetchSchemaConnectivity,
  fetchSchemaHashes,
  fetchStoredWasmSchema,
  type StoredPermissionsHead,
} from "../runtime/schema-fetch.js";
import { renderMigrationStub } from "./migrations.js";
import { normalizeSchemaHashInput } from "./schema-utils.js";
import {
  computeSchemaHash,
  deploy as deployCatalogue,
  MissingMigrationError,
  pushMigration as pushCatalogueMigration,
  pushPermissions as pushCataloguePermissions,
  pushSchema as pushCatalogueSchema,
  resolveKnownSchemaHash,
  resolveStoredStructuralSchemaHash,
  resolveStoredStructuralSchemaHashOrThrow,
  schemaTransitionRequiresRowTransform,
  shortSchemaHash,
} from "./catalogue.js";

export { shortSchemaHash };

export type CatalogueEvent =
  | { type: "schema-loaded"; schemaFile?: string }
  | { type: "schema-published"; hash: string; objectId?: string }
  | { type: "schema-skipped"; hash: string; reason: "already-stored" }
  | { type: "permissions-loaded"; permissionsFile?: string }
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
    | { status: "already-connected"; fromHash: string; toHash: string }
    | { status: "missing"; fromHash: string; toHash: string };
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

export interface ValidateProjectOptions {
  schemaDir: string;
}

export interface ValidateProjectResult {
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

interface ResolvedProjectDeployMigrationChain {
  migrations: Array<{
    migration: DefinedMigration;
    filePath: string;
    fromHash: string;
    toHash: string;
  }>;
}

type DeployCatalogueResult = Awaited<ReturnType<typeof deployCatalogue>>;
type ProjectDeployCatalogueResult = Omit<DeployCatalogueResult, "migration"> & {
  migration?: DeployResult["migration"];
};

function emit(options: { onEvent?: (event: CatalogueEvent) => void }, event: CatalogueEvent): void {
  options.onEvent?.(event);
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
    warnings: [
      ...collectConventionalProvenanceDiagnostics(compiled.schema),
      ...collectMissingExplicitPolicyDiagnostics(
        compiled.schema.tables.map((table) => table.name),
        compiled.permissions,
      ).map((diagnostic) => diagnostic.message),
    ],
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

  const result = await pushCatalogueSchema({
    appId: options.appId,
    serverUrl: options.serverUrl,
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

  const result = await pushCataloguePermissions({
    appId: options.appId,
    serverUrl: options.serverUrl,
    adminSecret: options.adminSecret,
    schemaHash: options.schemaHash,
    permissions: compiled.permissions,
  });
  emit(options, {
    type: "permissions-published",
    schemaHash: result.schemaHash,
    version: result.head?.version,
  });

  return {
    schemaHash: result.schemaHash,
    permissionsFile: compiled.permissionsFile,
    previousHead: result.previousHead,
    head: result.head,
  };
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

/**
 * Node does not expose openat(2), but Linux exposes an equivalent capability
 * through /proc/self/fd. Every later path is rooted at an already-open
 * directory descriptor, so renaming a checked directory to a symlink cannot
 * redirect a read or publication. Other platforms fail closed until they have
 * a descriptor-relative implementation.
 */
interface SecureMigrationDirectory {
  handle: FileHandle;
  path: string;
}

function descriptorPath(handle: FileHandle, entry?: string): string {
  const root = `/proc/self/fd/${handle.fd}`;
  return entry === undefined ? root : join(root, entry);
}

function assertSafeSinglePathComponent(entry: string): void {
  if (!entry || entry !== basename(entry) || entry === "." || entry === "..") {
    throw new Error(`Unsafe migration directory entry: ${entry}`);
  }
}

function rethrowUnsafeMigrationPath(path: string, error: unknown): never {
  const code = (error as NodeJS.ErrnoException).code;
  if (code === "ELOOP" || code === "ENOTDIR") {
    throw new Error(`Migration path must not contain a symlink or junction: ${path}`, {
      cause: error,
    });
  }
  throw error;
}

async function openDirectoryAt(
  parent: FileHandle,
  entry: string,
  create: boolean,
): Promise<FileHandle> {
  assertSafeSinglePathComponent(entry);
  const path = descriptorPath(parent, entry);
  const flags = fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW;
  try {
    return await open(path, flags);
  } catch (error) {
    if (!create || (error as NodeJS.ErrnoException).code !== "ENOENT") {
      rethrowUnsafeMigrationPath(path, error);
    }
    try {
      await mkdir(path);
      return await open(path, flags);
    } catch (createError) {
      rethrowUnsafeMigrationPath(path, createError);
    }
  }
}

async function openSecureMigrationDirectory(path: string): Promise<SecureMigrationDirectory> {
  if (process.platform !== "linux") {
    throw new Error(
      "Migration publication requires Linux descriptor-relative filesystem operations on this platform.",
    );
  }
  const absolute = resolve(path);
  const parsed = parse(absolute);
  let current = await open(
    parsed.root,
    fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
  );
  try {
    for (const component of absolute.slice(parsed.root.length).split(sep).filter(Boolean)) {
      const next = await openDirectoryAt(current, component, true);
      await current.close();
      current = next;
    }
    return { handle: current, path: descriptorPath(current) };
  } catch (error) {
    await current.close();
    throw error;
  }
}

async function readFileAtNoFollow(dir: FileHandle, entry: string): Promise<Buffer> {
  assertSafeSinglePathComponent(entry);
  const path = descriptorPath(dir, entry);
  let handle: FileHandle;
  try {
    handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  } catch (error) {
    rethrowUnsafeMigrationPath(path, error);
  }
  try {
    return await handle.readFile();
  } finally {
    await handle.close();
  }
}

async function snapshotDirectoryForMigrations(
  secureMigrationsDir: SecureMigrationDirectory,
  create = false,
): Promise<FileHandle | null> {
  try {
    return await openDirectoryAt(secureMigrationsDir.handle, "snapshots", create);
  } catch (error) {
    if (!create && (error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
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
  await assertNoSymlinkComponents(filePath);
  const schema = JSON.parse(await readFile(filePath, "utf8")) as WasmSchema;
  return {
    hash: await computeSchemaHash(schema),
    fileName,
    filePath,
    schema,
  };
}

async function readSecureSnapshotEntry(
  dir: FileHandle,
  fileName: string,
): Promise<SnapshotEntry | null> {
  if (!looksLikeSnapshotFileName(fileName)) return null;
  await pauseMigrationPublicationForTest("snapshot-read");
  const schema = JSON.parse(
    (await readFileAtNoFollow(dir, fileName)).toString("utf8"),
  ) as WasmSchema;
  return {
    hash: await computeSchemaHash(schema),
    fileName,
    filePath: descriptorPath(dir, fileName),
    schema,
  };
}

async function listSnapshotEntries(dir: string): Promise<SnapshotEntry[]> {
  if (!(await pathExists(dir))) {
    return [];
  }

  // Snapshots determine both the logical timestamp and the implicit baseline.
  // Do not let either decision follow a redirected committed snapshot path.
  await assertNoSymlinkComponents(dir);
  const files = await readdir(dir);
  return (await Promise.all(files.map((fileName) => readSnapshotEntry(dir, fileName)))).filter(
    (entry): entry is SnapshotEntry => entry !== null,
  );
}

async function listSnapshotEntriesForMigrations(
  migrationsDir: string,
  secureMigrationsDir?: SecureMigrationDirectory,
): Promise<SnapshotEntry[]> {
  if (secureMigrationsDir) {
    const dir = await snapshotDirectoryForMigrations(secureMigrationsDir);
    if (!dir) return [];
    try {
      const files = await readdir(descriptorPath(dir));
      return (
        await Promise.all(files.map((fileName) => readSecureSnapshotEntry(dir, fileName)))
      ).filter((entry): entry is SnapshotEntry => entry !== null);
    } finally {
      await dir.close();
    }
  }
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

function timestampDate(timestamp: string): Date {
  const match = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})$/.exec(timestamp);
  if (!match) {
    throw new Error(`Invalid migration timestamp: ${timestamp}`);
  }
  return new Date(
    Date.UTC(
      Number(match[1]),
      Number(match[2]) - 1,
      Number(match[3]),
      Number(match[4]),
      Number(match[5]),
      Number(match[6]),
    ),
  );
}

async function nextCommittedSnapshotTimestamp(
  migrationsDir: string,
  secureMigrationsDir?: SecureMigrationDirectory,
  now: Date = new Date(),
): Promise<string> {
  const current = createTimestamp(now);
  const latest = (await listSnapshotEntriesForMigrations(migrationsDir, secureMigrationsDir))
    .filter((entry) => isCommittedSnapshotFileName(entry.fileName))
    .map((entry) => /^([0-9]{8}T[0-9]{6})-/.exec(entry.fileName)?.[1])
    .filter((timestamp): timestamp is string => timestamp !== undefined)
    .sort()
    .at(-1);
  if (!latest || current > latest) {
    return current;
  }
  return createTimestamp(new Date(timestampDate(latest).getTime() + 1000));
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
  secureMigrationsDir?: SecureMigrationDirectory,
): Promise<string> {
  if (secureMigrationsDir) {
    const dir = await snapshotDirectoryForMigrations(secureMigrationsDir, true);
    if (!dir) throw new Error("Migration snapshots directory does not exist");
    const contents = `${JSON.stringify(schema, null, 2)}\n`;
    try {
      await pauseMigrationPublicationForTest("remote-cache-write");
      try {
        await syncFile(descriptorPath(dir, fileName), contents);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        const identity = contentIdentity(contents);
        await verifiedContentsAt(dir, fileName, identity.size, identity.sha256);
      }
    } finally {
      await dir.close();
    }
    return join(snapshotsDirForMigrations(migrationsDir), fileName);
  }
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
  secureMigrationsDir?: SecureMigrationDirectory,
): Promise<ResolvedSchemaInput | null> {
  const entries = await listSnapshotEntriesForMigrations(migrationsDir, secureMigrationsDir);
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

interface MigrationPublicationFile {
  finalPath: string;
  contents: string;
}

interface MigrationPublicationJournal {
  version: 2;
  files: Array<{
    stagedName: string;
    finalRelativePath: string;
    size: number;
    sha256: string;
  }>;
}

const MIGRATION_STAGE_DIR = ".jazz-create-migration-stage";
const MIGRATION_JOURNAL = ".jazz-create-migration.journal.json";

interface MigrationLockOwner {
  version: 1;
  pid: number;
  hostname: string;
  token: string;
}

function parseMigrationLockOwner(text: string): MigrationLockOwner {
  const value = JSON.parse(text) as unknown;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("owner is not an object");
  }
  const owner = value as Record<string, unknown>;
  if (
    Object.keys(owner).sort().join(",") !== "hostname,pid,token,version" ||
    owner.version !== 1 ||
    !Number.isSafeInteger(owner.pid) ||
    (owner.pid as number) <= 0 ||
    typeof owner.hostname !== "string" ||
    owner.hostname.length === 0 ||
    typeof owner.token !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(owner.token)
  ) {
    throw new Error("owner has an invalid shape");
  }
  return owner as unknown as MigrationLockOwner;
}

function contentIdentity(contents: string | Buffer): { size: number; sha256: string } {
  const bytes = Buffer.isBuffer(contents) ? contents : Buffer.from(contents);
  return { size: bytes.byteLength, sha256: createHash("sha256").update(bytes).digest("hex") };
}

async function assertNoSymlinkComponents(path: string, allowMissingTail = false): Promise<void> {
  const absolute = resolve(path);
  const parsedRoot = parse(absolute).root;
  const components = absolute.slice(parsedRoot.length).split(sep).filter(Boolean);
  let current = parsedRoot;
  for (const component of components) {
    current = join(current, component);
    try {
      const stat = await lstat(current);
      if (stat.isSymbolicLink()) {
        throw new Error(`Migration path must not contain a symlink or junction: ${current}`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT" && allowMissingTail) return;
      throw error;
    }
  }
}

async function verifiedContents(path: string, expectedSize: number, expectedSha256: string) {
  await assertNoSymlinkComponents(path);
  const contents = await readFile(path);
  const actual = contentIdentity(contents);
  if (actual.size !== expectedSize || actual.sha256 !== expectedSha256) {
    throw new Error(`Migration publication content does not match its journal: ${path}`);
  }
}

async function verifiedContentsAt(
  dir: FileHandle,
  entry: string,
  expectedSize: number,
  expectedSha256: string,
): Promise<void> {
  const contents = await readFileAtNoFollow(dir, entry);
  const actual = contentIdentity(contents);
  if (actual.size !== expectedSize || actual.sha256 !== expectedSha256) {
    throw new Error(
      `Migration publication content does not match its journal: ${descriptorPath(dir, entry)}`,
    );
  }
}

async function publishStagedFileNoReplace(
  stage: FileHandle,
  stagedName: string,
  destination: SecurePublicationDestination,
  size: number,
  sha256: string,
): Promise<void> {
  const stagedPath = descriptorPath(stage, stagedName);
  const destinationPath = descriptorPath(destination.dir, destination.fileName);
  try {
    // link(2) is an atomic no-replace publication primitive. Unlike rename,
    // it cannot overwrite a file created after the journal was committed.
    await link(stagedPath, destinationPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    // A retry/recovery may find its own already-published generation. It may
    // only adopt that destination when the journal identity matches exactly.
    await verifiedContentsAt(destination.dir, destination.fileName, size, sha256);
  }
  await unlink(stagedPath);
}

function relativePublicationPath(migrationsDir: string, finalPath: string): string {
  const relativePath = relative(resolve(migrationsDir), resolve(finalPath));
  if (
    !relativePath ||
    relativePath === ".." ||
    relativePath.startsWith(`..${sep}`) ||
    isAbsolute(relativePath)
  ) {
    throw new Error(`Migration publication escaped its directory: ${finalPath}`);
  }
  return relativePath;
}

interface SecurePublicationDestination {
  dir: FileHandle;
  fileName: string;
  closeWhenDone: boolean;
}

async function securePublicationDestination(
  secureMigrationsDir: SecureMigrationDirectory,
  relativePath: string,
  createParent: boolean,
): Promise<SecurePublicationDestination> {
  const components = relativePath.split(sep);
  if (components.length === 1) {
    assertSafeSinglePathComponent(components[0]!);
    return { dir: secureMigrationsDir.handle, fileName: components[0]!, closeWhenDone: false };
  }
  if (components.length === 2 && components[0] === "snapshots") {
    assertSafeSinglePathComponent(components[1]!);
    const snapshots = await snapshotDirectoryForMigrations(secureMigrationsDir, createParent);
    if (!snapshots) throw new Error("Migration snapshots directory does not exist");
    return { dir: snapshots, fileName: components[1]!, closeWhenDone: true };
  }
  throw new Error(`Invalid migration publication path: ${relativePath}`);
}

async function closeSecurePublicationDestination(destination: SecurePublicationDestination) {
  if (destination.closeWhenDone) await destination.dir.close();
}

async function noFollowPathExists(dir: FileHandle, entry: string): Promise<boolean> {
  try {
    const handle = await open(
      descriptorPath(dir, entry),
      fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW,
    );
    await handle.close();
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    rethrowUnsafeMigrationPath(descriptorPath(dir, entry), error);
  }
}

async function syncFile(path: string, contents: string): Promise<void> {
  const handle = await open(path, "wx");
  try {
    await handle.writeFile(contents);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function syncDirectory(path: string): Promise<void> {
  if (process.platform === "win32") return;
  const handle = await open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function recoverSecureMigrationPublication(
  migrationsDir: string,
  secureMigrationsDir: SecureMigrationDirectory,
): Promise<void> {
  const root = secureMigrationsDir.handle;
  if (!(await noFollowPathExists(root, MIGRATION_JOURNAL))) {
    const stagePath = descriptorPath(root, MIGRATION_STAGE_DIR);
    try {
      const stat = await lstat(stagePath);
      if (stat.isSymbolicLink()) {
        throw new Error(`Migration path must not contain a symlink or junction: ${stagePath}`);
      }
      await rm(stagePath, { recursive: true, force: true });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    for (const entry of await readdir(descriptorPath(root))) {
      if (entry.startsWith(`${MIGRATION_JOURNAL}.`) && entry.endsWith(".tmp")) {
        await rm(descriptorPath(root, entry), { force: true });
      }
    }
    return;
  }

  const journalPath = join(migrationsDir, MIGRATION_JOURNAL);
  const journal = JSON.parse(
    (await readFileAtNoFollow(root, MIGRATION_JOURNAL)).toString("utf8"),
  ) as MigrationPublicationJournal;
  if (journal.version !== 2 || !Array.isArray(journal.files) || journal.files.length === 0) {
    throw new Error(`Invalid interrupted migration publication journal: ${journalPath}`);
  }

  let stage: FileHandle | null = null;
  try {
    for (const file of journal.files) {
      if (basename(file.stagedName) !== file.stagedName) {
        throw new Error(`Invalid staged migration filename in ${journalPath}`);
      }
      const finalPath = resolve(migrationsDir, file.finalRelativePath);
      if (relativePublicationPath(migrationsDir, finalPath) !== file.finalRelativePath) {
        throw new Error(`Invalid migration publication path in ${journalPath}`);
      }
      if (
        !Number.isSafeInteger(file.size) ||
        file.size < 0 ||
        !/^[0-9a-f]{64}$/.test(file.sha256)
      ) {
        throw new Error(`Invalid migration publication identity in ${journalPath}`);
      }
      const destination = await securePublicationDestination(
        secureMigrationsDir,
        file.finalRelativePath,
        true,
      );
      try {
        if (await noFollowPathExists(destination.dir, destination.fileName)) {
          await verifiedContentsAt(destination.dir, destination.fileName, file.size, file.sha256);
          continue;
        }
        stage ??= await openDirectoryAt(root, MIGRATION_STAGE_DIR, false);
        if (!(await noFollowPathExists(stage, file.stagedName))) {
          throw new Error(
            `Cannot recover interrupted migration publication: missing ${join(migrationsDir, MIGRATION_STAGE_DIR, file.stagedName)} and ${finalPath}`,
          );
        }
        await verifiedContentsAt(stage, file.stagedName, file.size, file.sha256);
        await publishStagedFileNoReplace(
          stage,
          file.stagedName,
          destination,
          file.size,
          file.sha256,
        );
        await verifiedContentsAt(destination.dir, destination.fileName, file.size, file.sha256);
        await syncDirectory(descriptorPath(destination.dir));
      } finally {
        await closeSecurePublicationDestination(destination);
      }
    }
  } finally {
    if (stage) await stage.close();
  }

  await rm(descriptorPath(root, MIGRATION_JOURNAL), { force: true });
  await syncDirectory(descriptorPath(root));
  await rm(descriptorPath(root, MIGRATION_STAGE_DIR), { recursive: true, force: true });
  await syncDirectory(descriptorPath(root));
}

async function pauseMigrationPublicationForTest(phase: string): Promise<void> {
  if (process.env.NODE_ENV !== "test" || process.env.JAZZ_TEST_MIGRATION_PAUSE_AT !== phase) {
    return;
  }
  const marker = process.env.JAZZ_TEST_MIGRATION_PAUSE_MARKER;
  if (marker) await writeFile(marker, phase);
  const releaseMarker = process.env.JAZZ_TEST_MIGRATION_PAUSE_RELEASE_MARKER;
  if (releaseMarker) {
    while (!(await pathExists(releaseMarker))) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    return;
  }
  await new Promise<never>(() => {
    setInterval(() => undefined, 1_000);
  });
}

/**
 * Publish one logical migration generation. Multiple renames cannot be atomic,
 * so a crash may briefly expose a prefix of the files. The fsynced journal is
 * written first; every later invocation recovers that exact generation under
 * the directory lock before inspecting baselines or allocating a timestamp.
 */
async function publishMigrationFilesRecoverably(
  migrationsDir: string,
  files: MigrationPublicationFile[],
  secureMigrationsDir?: SecureMigrationDirectory,
): Promise<void> {
  if (files.length === 0) return;
  const stage = secureMigrationsDir
    ? await openDirectoryAt(secureMigrationsDir.handle, MIGRATION_STAGE_DIR, true)
    : null;
  const stageDir = stage ? descriptorPath(stage) : join(migrationsDir, MIGRATION_STAGE_DIR);
  const stageRemovalPath = secureMigrationsDir
    ? descriptorPath(secureMigrationsDir.handle, MIGRATION_STAGE_DIR)
    : stageDir;
  const journalPath = secureMigrationsDir
    ? descriptorPath(secureMigrationsDir.handle, MIGRATION_JOURNAL)
    : join(migrationsDir, MIGRATION_JOURNAL);
  try {
    if (!secureMigrationsDir) {
      await assertNoSymlinkComponents(migrationsDir);
      await assertNoSymlinkComponents(journalPath, true);
      await mkdir(stageDir, { recursive: true });
      await assertNoSymlinkComponents(stageDir);
    }
    const journal: MigrationPublicationJournal = { version: 2, files: [] };
    for (const [index, file] of files.entries()) {
      const finalRelativePath = relativePublicationPath(migrationsDir, file.finalPath);
      if (!secureMigrationsDir) {
        await assertNoSymlinkComponents(dirname(file.finalPath), true);
        await assertNoSymlinkComponents(file.finalPath, true);
        if (await pathExists(file.finalPath)) {
          throw new Error(`Migration output already exists: ${file.finalPath}`);
        }
      } else {
        const destination = await securePublicationDestination(
          secureMigrationsDir,
          finalRelativePath,
          true,
        );
        try {
          if (await noFollowPathExists(destination.dir, destination.fileName)) {
            throw new Error(`Migration output already exists: ${file.finalPath}`);
          }
        } finally {
          await closeSecurePublicationDestination(destination);
        }
      }
      const stagedName = `${randomUUID()}-${index}`;
      const stagedPath = join(stageDir, stagedName);
      await syncFile(stagedPath, file.contents);
      const identity = contentIdentity(file.contents);
      if (stage) {
        await verifiedContentsAt(stage, stagedName, identity.size, identity.sha256);
      } else {
        await verifiedContents(stagedPath, identity.size, identity.sha256);
      }
      journal.files.push({
        stagedName,
        finalRelativePath,
        ...identity,
      });
    }
    await syncDirectory(stageDir);
    const journalTemp = `${journalPath}.${randomUUID()}.tmp`;
    await syncFile(journalTemp, `${JSON.stringify(journal)}\n`);
    await rename(journalTemp, journalPath);
    await syncDirectory(
      secureMigrationsDir ? descriptorPath(secureMigrationsDir.handle) : migrationsDir,
    );
    await pauseMigrationPublicationForTest("journaled");
    for (const [index, file] of journal.files.entries()) {
      const finalPath = resolve(migrationsDir, file.finalRelativePath);
      const stagedPath = join(stageDir, file.stagedName);
      if (!secureMigrationsDir) {
        await assertNoSymlinkComponents(dirname(finalPath), true);
        await mkdir(dirname(finalPath), { recursive: true });
        await assertNoSymlinkComponents(dirname(finalPath));
        await verifiedContents(stagedPath, file.size, file.sha256);
        await rename(stagedPath, finalPath);
        await verifiedContents(finalPath, file.size, file.sha256);
        await syncDirectory(dirname(finalPath));
      } else {
        const destination = await securePublicationDestination(
          secureMigrationsDir,
          file.finalRelativePath,
          true,
        );
        try {
          // The descriptor-held parent prevents a symlink swap between the
          // preflight checks and this rename. The staged source is also anchored
          // below the descriptor-held migration root.
          await verifiedContentsAt(stage!, file.stagedName, file.size, file.sha256);
          await publishStagedFileNoReplace(
            stage!,
            file.stagedName,
            destination,
            file.size,
            file.sha256,
          );
          await verifiedContentsAt(destination.dir, destination.fileName, file.size, file.sha256);
          await syncDirectory(descriptorPath(destination.dir));
        } finally {
          await closeSecurePublicationDestination(destination);
        }
      }
      if (index === 0 && journal.files.length > 1) {
        await pauseMigrationPublicationForTest("between-publications");
      }
    }
    await rm(journalPath, { force: true });
    await syncDirectory(
      secureMigrationsDir ? descriptorPath(secureMigrationsDir.handle) : migrationsDir,
    );
    await rm(stageRemovalPath, { recursive: true, force: true });
  } finally {
    await stage?.close();
  }
}

async function committedSnapshotPublication(
  migrationsDir: string,
  schema: ResolvedSchemaInput,
  timestamp: string,
  secureMigrationsDir?: SecureMigrationDirectory,
): Promise<MigrationPublicationFile | null> {
  const entries = await listSnapshotEntriesForMigrations(migrationsDir, secureMigrationsDir);
  if (
    entries.some(
      (entry) => entry.hash === schema.hash && isCommittedSnapshotFileName(entry.fileName),
    )
  ) {
    return null;
  }
  return {
    finalPath: join(
      snapshotsDirForMigrations(migrationsDir),
      snapshotFilename(schema.hash, timestamp),
    ),
    contents: `${JSON.stringify(schema.schema, null, 2)}\n`,
  };
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
  secureMigrationsDir: SecureMigrationDirectory,
  hash: string,
  label: string,
  appId: string | undefined,
  serverUrl: string | undefined,
  adminSecret: string | undefined,
): Promise<ResolvedSchemaInput> {
  const local = await resolveLocalHistoricalSchema(migrationsDir, hash, label, secureMigrationsDir);
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
    secureMigrationsDir,
  );
}

async function createMigrationUnlocked(
  options: CreateMigrationOptions,
  secureMigrationsDir: SecureMigrationDirectory,
): Promise<CreateMigrationResult> {
  const explicitHashFlow = Boolean(options.fromHash || options.toHash);

  const currentSchema =
    !explicitHashFlow || !options.toHash ? await loadCurrentSchema(options.schemaDir) : null;

  let fromSchema: ResolvedSchemaInput;
  let toSchema: ResolvedSchemaInput;
  let shouldWriteCommittedSnapshot = false;
  // Snapshot filenames are the ordering source for the implicit migration
  // baseline. Treat their second-resolution timestamp as a logical clock so
  // rapid successive schema edits cannot let the hash suffix pick the winner.
  const timestamp = await nextCommittedSnapshotTimestamp(
    options.migrationsDir,
    secureMigrationsDir,
  );

  if (explicitHashFlow) {
    if (options.fromHash) {
      fromSchema = await resolveHistoricalSchemaForCreateMigration(
        options.migrationsDir,
        secureMigrationsDir,
        options.fromHash,
        "fromHash",
        options.appId,
        options.serverUrl,
        options.adminSecret,
      );
    } else {
      const latest = await loadLatestCommittedSnapshot(options.migrationsDir, secureMigrationsDir);
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
          secureMigrationsDir,
          options.toHash,
          "toHash",
          options.appId,
          options.serverUrl,
          options.adminSecret,
        )
      : currentSchema!;
    shouldWriteCommittedSnapshot = !options.toHash;
  } else {
    const latest = await loadLatestCommittedSnapshot(options.migrationsDir, secureMigrationsDir);
    if (!latest) {
      const snapshot = await committedSnapshotPublication(
        options.migrationsDir,
        currentSchema!,
        timestamp,
        secureMigrationsDir,
      );
      if (!snapshot) throw new Error("Initial committed snapshot already exists");
      await publishMigrationFilesRecoverably(
        options.migrationsDir,
        [snapshot],
        secureMigrationsDir,
      );
      return {
        status: "initial-snapshot",
        snapshotPath: snapshot.finalPath,
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
    const snapshot = shouldWriteCommittedSnapshot
      ? await committedSnapshotPublication(
          options.migrationsDir,
          toSchema,
          timestamp,
          secureMigrationsDir,
        )
      : null;
    if (snapshot)
      await publishMigrationFilesRecoverably(
        options.migrationsDir,
        [snapshot],
        secureMigrationsDir,
      );
    return {
      status: "migration-not-required",
      fromHash: fromSchema.hash,
      toHash: toSchema.hash,
      snapshotPath: snapshot?.finalPath ?? null,
    };
  }

  const filePath = migrationFilename(
    options.migrationsDir,
    fromSchema.hash,
    toSchema.hash,
    options.name ? normalizeMigrationName(options.name) : undefined,
    timestamp,
  );
  const stub = renderMigrationStub({
    fromHash: fromSchema.hash,
    toHash: toSchema.hash,
    fromSchema: fromSchema.schema,
    toSchema: toSchema.schema,
  });
  const snapshot = shouldWriteCommittedSnapshot
    ? await committedSnapshotPublication(
        options.migrationsDir,
        toSchema,
        timestamp,
        secureMigrationsDir,
      )
    : null;
  await publishMigrationFilesRecoverably(
    options.migrationsDir,
    [{ finalPath: filePath, contents: stub }, ...(snapshot ? [snapshot] : [])],
    secureMigrationsDir,
  );

  return {
    status: "generated",
    filePath,
    fromHash: fromSchema.hash,
    toHash: toSchema.hash,
    needsRename: !options.name,
    snapshotPath: snapshot?.finalPath ?? null,
  };
}

async function withMigrationDirectoryLock<T>(
  migrationsDir: string,
  secureMigrationsDir: SecureMigrationDirectory,
  operation: () => Promise<T>,
): Promise<T> {
  const lockName = ".jazz-create-migration.lock";
  const ownerName = "owner.json";
  const lockDir = join(migrationsDir, lockName);
  const root = secureMigrationsDir.handle;
  const owner: MigrationLockOwner = {
    version: 1,
    pid: process.pid,
    hostname: hostname(),
    token: randomUUID(),
  };
  const deadline = Date.now() + 10_000;
  let unknownOwnerSince: number | null = null;
  let heldLock: FileHandle | null = null;
  for (;;) {
    try {
      // mkdir is the no-replace cross-process compare-and-set. A process that
      // observes the short owner-write window fails closed rather than ever
      // treating an unknown lock as stale.
      await mkdir(descriptorPath(root, lockName));
      heldLock = await openDirectoryAt(root, lockName, false);
      await syncFile(descriptorPath(heldLock, ownerName), `${JSON.stringify(owner)}\n`);
      await syncDirectory(descriptorPath(heldLock));
      await syncDirectory(descriptorPath(root));
      await pauseMigrationPublicationForTest("lock-held");
      break;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST") throw error;
      let existingLock: FileHandle;
      try {
        existingLock = await openDirectoryAt(root, lockName, false);
      } catch (validationError) {
        if ((validationError as NodeJS.ErrnoException).code === "ENOENT") continue;
        throw validationError;
      }
      let existing: MigrationLockOwner | null = null;
      let existingText = "";
      try {
        existingText = (await readFileAtNoFollow(existingLock, ownerName)).toString("utf8");
        existing = parseMigrationLockOwner(existingText);
      } catch {
        await existingLock.close();
        if (!(await noFollowPathExists(root, lockName))) continue;
        unknownOwnerSince ??= Date.now();
        // A freshly created lock has a necessarily brief mkdir→owner-write
        // window. Wait for that owner, but never steal the lock if it remains
        // unknown: fail closed with the directory intact.
        if (Date.now() - unknownOwnerSince >= 500) {
          throw new Error(
            `Cannot safely acquire migration lock ${lockDir}; owner metadata is missing, invalid, or unsafe`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
        continue;
      }
      await existingLock.close();
      unknownOwnerSince = null;
      if (
        existing?.version === 1 &&
        existing.hostname === hostname() &&
        Number.isSafeInteger(existing.pid) &&
        existing.pid > 0 &&
        !processIsAlive(existing.pid)
      ) {
        const quarantine = `${lockDir}.recovery-${owner.token}`;
        try {
          await rename(descriptorPath(root, lockName), descriptorPath(root, basename(quarantine)));
        } catch (quarantineError) {
          if ((quarantineError as NodeJS.ErrnoException).code === "ENOENT") continue;
          throw quarantineError;
        }
        const quarantineName = basename(quarantine);
        const quarantinedLock = await openDirectoryAt(root, quarantineName, false);
        try {
          const quarantinedOwner = (await readFileAtNoFollow(quarantinedLock, ownerName)).toString(
            "utf8",
          );
          if (quarantinedOwner !== existingText) {
            throw new Error(`Quarantined migration lock owner did not match: ${quarantine}`);
          }
        } finally {
          await quarantinedLock.close();
        }
        await pauseMigrationPublicationForTest("lock-quarantined");
        await rm(descriptorPath(root, quarantineName), { recursive: true });
        await syncDirectory(descriptorPath(root));
        continue;
      }
      if (Date.now() >= deadline) {
        const detail = existing
          ? `owner pid=${existing.pid} host=${existing.hostname}`
          : "owner metadata is missing or invalid";
        throw new Error(
          `Timed out waiting for another migration generator to release ${lockDir}; ${detail}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  try {
    await recoverSecureMigrationPublication(migrationsDir, secureMigrationsDir);
    return await operation();
  } finally {
    const current = heldLock
      ? (await readFileAtNoFollow(heldLock, ownerName).catch(() => Buffer.alloc(0))).toString(
          "utf8",
        )
      : "";
    if (current === `${JSON.stringify(owner)}\n`) {
      await heldLock?.close();
      await rm(descriptorPath(root, lockName), { recursive: true, force: true });
      await syncDirectory(descriptorPath(root));
    } else {
      await heldLock?.close();
    }
  }
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

export async function createMigration(
  options: CreateMigrationOptions,
): Promise<CreateMigrationResult> {
  const secureMigrationsDir = await openSecureMigrationDirectory(options.migrationsDir);
  try {
    return await withMigrationDirectoryLock(options.migrationsDir, secureMigrationsDir, () =>
      createMigrationUnlocked(options, secureMigrationsDir),
    );
  } finally {
    await secureMigrationsDir.handle.close();
  }
}

function isDefinedMigration(value: unknown): value is DefinedMigration {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    (candidate.fromHash === undefined || typeof candidate.fromHash === "string") &&
    (candidate.toHash === undefined || typeof candidate.toHash === "string") &&
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
): Promise<string | undefined> {
  if (!(await pathExists(migrationsDir))) {
    return undefined;
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
    return undefined;
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
  return resolveSnapshotEntryFromEntries(await listSnapshotEntries(dir), hash, label);
}

function resolveSnapshotEntryFromEntries(
  entries: SnapshotEntry[],
  hash: string,
  label: string,
): SnapshotEntry | null {
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
  secureMigrationsDir?: SecureMigrationDirectory,
): Promise<ResolvedSchemaInput | null> {
  if (secureMigrationsDir) await pauseMigrationPublicationForTest("explicit-local-read");
  const localEntry = secureMigrationsDir
    ? await resolveSnapshotEntryFromEntries(
        await listSnapshotEntriesForMigrations(migrationsDir, secureMigrationsDir),
        hash,
        label,
      )
    : await resolveSnapshotEntry(snapshotsDirForMigrations(migrationsDir), hash, label);
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
  secureMigrationsDir?: SecureMigrationDirectory,
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
      secureMigrationsDir,
    );
    return { hash: resolvedHash, schema: storedSchema.schema };
  } catch (error) {
    if (error instanceof Error && /Schema fetch failed: 404/i.test(error.message)) {
      throw new Error(`No stored schema found for ${label} ${resolvedHash}.`);
    }
    throw error;
  }
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
  const filePath = await findMigrationFile(options.migrationsDir, fromHash, toHash);

  const migration = filePath ? await loadDefinedMigration(filePath) : null;

  let result: PushMigrationResult;
  try {
    result = await pushCatalogueMigration(
      migration
        ? {
            appId: options.appId,
            serverUrl: options.serverUrl,
            adminSecret: options.adminSecret,
            fromHash,
            toHash,
            migration,
          }
        : {
            appId: options.appId,
            serverUrl: options.serverUrl,
            adminSecret: options.adminSecret,
            fromHash,
            toHash,
          },
    );
  } catch (error) {
    if (error instanceof MissingMigrationError) {
      throw new Error(
        noMigrationFileMessage(options.appId, options.migrationsDir, error.fromHash, error.toHash),
      );
    }
    throw error;
  }
  const projectResult = { ...result, filePath };
  emit(options, {
    type: "migration-published",
    fromHash: projectResult.fromHash,
    toHash: projectResult.toHash,
    filePath: projectResult.filePath,
  });
  return projectResult;
}

function disconnectedSchemaMessage(appId: string, fromHash: string, toHash: string): string {
  const fromShortHash = shortSchemaHash(fromHash);
  const toShortHash = shortSchemaHash(toHash);
  return `The new permissions schema ${toShortHash} is not connected to the previous permissions schema ${fromShortHash} on the server. Reads and writes may fail until you push a migration. Run \`jazz-tools migrations create ${appId} --fromHash ${fromShortHash} --toHash ${toShortHash}\` to create a migration and then re-run this command.`;
}

function noMigrationFileMessage(
  appId: string,
  migrationsDir: string,
  fromHash: string,
  toHash: string,
): string {
  return `No migration file found in ${migrationsDir} for ${fromHash} -> ${toHash}. Run \`jazz-tools migrations create ${appId} --fromHash ${shortSchemaHash(fromHash)} --toHash ${shortSchemaHash(toHash)}\` first.`;
}

function emitDeployResult(
  options: { onEvent?: (event: CatalogueEvent) => void },
  result: ProjectDeployCatalogueResult,
  permissionsFile?: string,
): void {
  for (const warning of result.warnings) {
    emit(options, { type: "warning", message: warning });
  }

  if (result.schema.status === "published") {
    emit(options, {
      type: "schema-published",
      hash: result.schema.hash,
      objectId: result.schema.objectId,
    });
  } else {
    emit(options, {
      type: "schema-skipped",
      hash: result.schema.hash,
      reason: "already-stored",
    });
  }

  if (!result.permissions) {
    emit(options, { type: "permissions-skipped", reason: "missing-permissions-file" });
    return;
  }

  emit(options, { type: "permissions-loaded", permissionsFile });

  if (result.migration) {
    if (result.migration.status === "already-connected") {
      emit(options, {
        type: "migration-skipped",
        reason: "already-connected",
        fromHash: result.migration.fromHash,
        toHash: result.migration.toHash,
      });
    } else if (result.migration.status === "published") {
      emit(options, {
        type: "migration-published",
        fromHash: result.migration.fromHash,
        toHash: result.migration.toHash,
        filePath: result.migration.filePath,
      });
    }
  }

  emit(options, {
    type: "permissions-published",
    schemaHash: result.permissions.schemaHash,
    version: result.permissions.head?.version,
  });
}

async function hasLocalMigrationFiles(migrationsDir: string): Promise<boolean> {
  if (!(await pathExists(migrationsDir))) {
    return false;
  }

  return (await readdir(migrationsDir)).some((fileName) => fileName.endsWith(".ts"));
}

function migrationHashesFromFileName(fileName: string): { from: string; to: string } {
  const match = fileName.match(/-([0-9a-f]{12,64})-([0-9a-f]{12,64})\.ts$/i);
  if (!match) {
    throw new Error(
      `Migration filename ${fileName} must end in -<fromHash>-<toHash>.ts using at least 12 hex characters per hash.`,
    );
  }
  return { from: match[1]!, to: match[2]! };
}

async function resolveProjectDeployMigrationChain(
  options: DeployOptions,
  migrationsDir: string,
  compiled: LoadedSchemaProject,
): Promise<ResolvedProjectDeployMigrationChain | undefined> {
  if (!compiled.permissions || !(await hasLocalMigrationFiles(migrationsDir))) {
    return undefined;
  }

  const { head } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });
  if (!head) {
    return undefined;
  }

  const toHash =
    (await resolveStoredStructuralSchemaHash(
      options.appId,
      options.serverUrl,
      options.adminSecret,
      compiled.wasmSchema,
    )) ?? (await computeSchemaHash(compiled.wasmSchema));
  if (head.schemaHash === toHash) {
    return undefined;
  }

  const files = (await readdir(migrationsDir))
    .filter((fileName) => fileName.endsWith(".ts"))
    .sort();
  const [stored, snapshots] = await Promise.all([
    fetchSchemaHashes(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    }),
    listSnapshotEntriesForMigrations(migrationsDir),
  ]);
  const knownHashes = [
    ...new Set([...stored.hashes, ...snapshots.map(({ hash }) => hash), toHash]),
  ];
  const edges = await Promise.all(
    files.map(async (fileName) => {
      const filePath = join(migrationsDir, fileName);
      const migration = await loadDefinedMigration(filePath);
      const named = migrationHashesFromFileName(fileName);
      const fromHash = resolveKnownSchemaHash(named.from, `fromHash in ${fileName}`, knownHashes);
      const toHash = resolveKnownSchemaHash(named.to, `toHash in ${fileName}`, knownHashes);

      return { migration, filePath, fromHash, toHash };
    }),
  );

  const outgoing = new Map<string, typeof edges>();
  for (const edge of edges) {
    const existing = outgoing.get(edge.fromHash) ?? [];
    existing.push(edge);
    outgoing.set(edge.fromHash, existing);
  }

  const paths: Array<typeof edges> = [];
  const visit = (at: string, path: typeof edges, visited: ReadonlySet<string>): void => {
    if (paths.length > 1) return;
    if (at === toHash) {
      paths.push(path);
      return;
    }
    for (const edge of outgoing.get(at) ?? []) {
      if (!visited.has(edge.toHash)) {
        visit(edge.toHash, [...path, edge], new Set([...visited, edge.toHash]));
      }
    }
  };
  visit(head.schemaHash, [], new Set([head.schemaHash]));

  if (paths.length > 1) {
    throw new Error(
      `Multiple local migration chains connect ${shortSchemaHash(head.schemaHash)} to ${shortSchemaHash(toHash)}. Keep exactly one reviewed path.`,
    );
  }
  return paths[0] ? { migrations: paths[0] } : undefined;
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
  const resolvedChain = await resolveProjectDeployMigrationChain(options, migrationsDir, compiled);
  const releaseHash = await computeSchemaHash(compiled.wasmSchema);

  if (resolvedChain) {
    // The catalogue deploy primitive accepts one migration.  Project deploy
    // deliberately owns multi-step replay so each reviewed edge is published
    // in order, and permissions cannot advance until the complete path exists.
    const existingReleaseSchema = await resolveStoredStructuralSchemaHash(
      options.appId,
      options.serverUrl,
      options.adminSecret,
      compiled.wasmSchema,
    );
    const warnings = collectMissingExplicitPolicyDiagnostics(
      Object.keys(compiled.wasmSchema),
      compiled.permissions,
    ).map((diagnostic) => diagnostic.message);
    let schema:
      | { hash: string; status: "published" | "already-stored"; objectId?: string }
      | undefined = existingReleaseSchema
      ? { hash: existingReleaseSchema, status: "already-stored" as const }
      : undefined;
    const migrationResults: Array<{
      migration: Awaited<ReturnType<typeof pushCatalogueMigration>>;
      filePath: string;
    }> = [];
    const snapshots = await listSnapshotEntriesForMigrations(migrationsDir);

    for (const edge of resolvedChain.migrations) {
      const storedTarget = (
        await fetchSchemaHashes(options.serverUrl, {
          appId: options.appId,
          adminSecret: options.adminSecret,
        })
      ).hashes.includes(edge.toHash);
      if (!storedTarget) {
        const targetSchema =
          edge.toHash === releaseHash
            ? compiled.wasmSchema
            : snapshots.find(({ hash }) => hash === edge.toHash)?.schema;
        if (!targetSchema) {
          throw new Error(
            `No stored schema or local snapshot found for intermediate migration target ${shortSchemaHash(edge.toHash)}.`,
          );
        }
        const published = await pushCatalogueSchema({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          schema: targetSchema,
        });
        if (published.hash !== edge.toHash) {
          throw new Error(
            `Published schema hash ${published.hash} did not match migration target ${edge.toHash}.`,
          );
        }
        if (edge.toHash === releaseHash) schema = published;
      }

      const { connected } = await fetchSchemaConnectivity(options.serverUrl, {
        appId: options.appId,
        adminSecret: options.adminSecret,
        fromHash: edge.fromHash,
        toHash: edge.toHash,
      });
      if (!connected) {
        const migration = await pushCatalogueMigration({
          appId: options.appId,
          serverUrl: options.serverUrl,
          adminSecret: options.adminSecret,
          migration: edge.migration,
          fromHash: edge.fromHash,
          toHash: edge.toHash,
        });
        migrationResults.push({ migration, filePath: edge.filePath });
      }
    }

    const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    });
    if (!previousHead) {
      throw new Error("Permissions head disappeared while replaying local migration chain.");
    }
    const permissions = await pushCataloguePermissions({
      appId: options.appId,
      serverUrl: options.serverUrl,
      adminSecret: options.adminSecret,
      schemaHash: releaseHash,
      permissions: compiled.permissions!,
    });

    if (!schema) {
      throw new Error(
        `Migration chain did not reach release schema ${shortSchemaHash(releaseHash)}.`,
      );
    }
    for (const warning of warnings) emit(options, { type: "warning", message: warning });
    emit(
      options,
      schema.status === "published"
        ? { type: "schema-published", hash: schema.hash, objectId: schema.objectId }
        : { type: "schema-skipped", hash: schema.hash, reason: "already-stored" },
    );
    emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });
    for (const { migration, filePath } of migrationResults) {
      emit(options, {
        type: "migration-published",
        fromHash: migration.fromHash,
        toHash: migration.toHash,
        filePath,
      });
    }
    emit(options, {
      type: "permissions-published",
      schemaHash: permissions.schemaHash,
      version: permissions.head?.version,
    });

    return {
      schema: { ...schema, schemaFile: compiled.schemaFile },
      migration: migrationResults.at(-1)?.migration,
      permissions: { ...permissions, permissionsFile: compiled.permissionsFile! },
      warnings,
    };
  }

  let result = await deployCatalogue({
    appId: options.appId,
    serverUrl: options.serverUrl,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
    permissions: compiled.permissions,
    noVerify: options.noVerify,
  });

  if (result.migration?.status === "missing") {
    const message = disconnectedSchemaMessage(
      options.appId,
      result.migration.fromHash,
      result.migration.toHash,
    );
    if (!options.noVerify) {
      throw new Error(message);
    }
    result = {
      ...result,
      warnings: [...result.warnings, message],
    };
  }

  emitDeployResult(options, result, compiled.permissionsFile);

  return {
    ...result,
    schema: {
      ...result.schema,
      schemaFile: compiled.schemaFile,
    },
    permissions:
      result.permissions && compiled.permissionsFile
        ? {
            ...result.permissions,
            permissionsFile: compiled.permissionsFile,
          }
        : undefined,
  };
}

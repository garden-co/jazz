/**
 * Contains utilities for deploying schemas, permissions, and migrations to a Jazz server.
 *
 * These are internal utilities for the CLI and dev tooling, which read/write schemas,
 * permissions, and migrations from/to the FS.
 *
 * Prefer using {@link catalogue.ts} utils whenever possible.
 */

import {
  access,
  link,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  rename,
  rm,
  unlink,
  writeFile,
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
  assertMigrationMatchesCanonicalBundle,
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

/**
 * Portable storage boundary for schema snapshots and migration publications.
 *
 * The project directory is trusted against mutation by another process running
 * as the same OS user: such a process can already rewrite project source and is
 * not an authorization boundary. Within that boundary we validate symlinks
 * before every operation, fail closed when one is observed, serialize Jazz
 * writers, and use atomic no-replace publication so concurrent Jazz commands
 * never clobber an existing file.
 */
class MigrationStorage {
  readonly migrationsDir: string;

  constructor(migrationsDir: string) {
    this.migrationsDir = resolve(migrationsDir);
  }

  get snapshotsDir(): string {
    return join(this.migrationsDir, "snapshots");
  }

  async initialize(): Promise<void> {
    await assertNoSymlinkComponents(this.migrationsDir, true);
    await mkdir(this.migrationsDir, { recursive: true });
    await assertNoSymlinkComponents(this.migrationsDir);
  }

  async listSnapshots(): Promise<SnapshotEntry[]> {
    return listSnapshotEntries(this.snapshotsDir);
  }

  async resolveSnapshot(hash: string, label: string): Promise<SnapshotEntry | null> {
    return resolveSnapshotEntry(this.snapshotsDir, hash, label);
  }

  async writeSnapshot(fileName: string, schema: WasmSchema): Promise<string> {
    if (basename(fileName) !== fileName || !looksLikeSnapshotFileName(fileName)) {
      throw new Error(`Invalid snapshot filename: ${fileName}`);
    }
    await assertNoSymlinkComponents(this.snapshotsDir, true);
    await mkdir(this.snapshotsDir, { recursive: true });
    await assertNoSymlinkComponents(this.snapshotsDir);
    const filePath = join(this.snapshotsDir, fileName);
    await assertNoSymlinkComponents(filePath, true);
    const contents = `${JSON.stringify(schema, null, 2)}\n`;
    try {
      await syncFile(filePath, contents);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const identity = contentIdentity(contents);
      await verifiedContents(filePath, identity.size, identity.sha256);
    }
    return filePath;
  }
}

// Schema snapshots are named from their Unix-millisecond publication time.
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
  storage: MigrationStorage,
): Promise<SnapshotEntry[]> {
  return storage.listSnapshots();
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
  storage: MigrationStorage,
  now: Date = new Date(),
): Promise<string> {
  const current = createTimestamp(now);
  const latest = (await listSnapshotEntriesForMigrations(storage))
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
  storage: MigrationStorage,
  fileName: string,
  schema: WasmSchema,
): Promise<string> {
  return storage.writeSnapshot(fileName, schema);
}

function defaultMigrationsDir(schemaDir: string): string {
  return join(schemaDir, "migrations");
}

function resolvedMigrationsDir(schemaDir: string, migrationsDir?: string): string {
  return migrationsDir ?? defaultMigrationsDir(schemaDir);
}

async function listLocalSnapshotEntries(
  schemaDir: string,
  migrationsDir?: string,
): Promise<SnapshotEntry[]> {
  const storage = new MigrationStorage(resolvedMigrationsDir(schemaDir, migrationsDir));
  await storage.initialize();
  return storage.listSnapshots();
}

async function resolveLocalSnapshotEntry(
  schemaDir: string,
  migrationsDir: string | undefined,
  hash: string,
  label: string,
): Promise<SnapshotEntry | null> {
  const storage = new MigrationStorage(resolvedMigrationsDir(schemaDir, migrationsDir));
  await storage.initialize();
  return storage.resolveSnapshot(hash, label);
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
  const storage = new MigrationStorage(resolvedMigrationsDir(schemaDir, migrationsDir));
  await storage.initialize();
  return storage.writeSnapshot(snapshotFilename(hash, timestamp), schema);
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
  storage: MigrationStorage,
): Promise<ResolvedSchemaInput | null> {
  const entries = await listSnapshotEntriesForMigrations(storage);
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

async function publishStagedFileNoReplace(
  stagedPath: string,
  finalPath: string,
  expectedSize: number,
  expectedSha256: string,
): Promise<void> {
  try {
    // Portable atomic no-replace publication: a concurrent Jazz writer can
    // create the destination first, but this operation never overwrites it.
    await link(stagedPath, finalPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    // Recovery may observe its own already-published output. Only adopt it
    // when it matches the fsynced journal identity exactly.
    await verifiedContents(finalPath, expectedSize, expectedSha256);
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

async function recoverMigrationPublication(storage: MigrationStorage): Promise<void> {
  const migrationsDir = storage.migrationsDir;
  await assertNoSymlinkComponents(migrationsDir);
  const journalPath = join(migrationsDir, MIGRATION_JOURNAL);
  if (!(await pathExists(journalPath))) {
    const stageDir = join(migrationsDir, MIGRATION_STAGE_DIR);
    await assertNoSymlinkComponents(stageDir, true);
    await rm(stageDir, { recursive: true, force: true });
    for (const entry of await readdir(migrationsDir)) {
      if (entry.startsWith(`${MIGRATION_JOURNAL}.`) && entry.endsWith(".tmp")) {
        await rm(join(migrationsDir, entry), { force: true });
      }
    }
    return;
  }
  await assertNoSymlinkComponents(journalPath);
  const journal = JSON.parse(await readFile(journalPath, "utf8")) as MigrationPublicationJournal;
  if (journal.version !== 2 || !Array.isArray(journal.files) || journal.files.length === 0) {
    throw new Error(`Invalid interrupted migration publication journal: ${journalPath}`);
  }
  for (const file of journal.files) {
    if (basename(file.stagedName) !== file.stagedName) {
      throw new Error(`Invalid staged migration filename in ${journalPath}`);
    }
    const stagedPath = join(migrationsDir, MIGRATION_STAGE_DIR, file.stagedName);
    const finalPath = resolve(migrationsDir, file.finalRelativePath);
    if (relativePublicationPath(migrationsDir, finalPath) !== file.finalRelativePath) {
      throw new Error(`Invalid migration publication path in ${journalPath}`);
    }
    if (!Number.isSafeInteger(file.size) || file.size < 0 || !/^[0-9a-f]{64}$/.test(file.sha256)) {
      throw new Error(`Invalid migration publication identity in ${journalPath}`);
    }
    await assertNoSymlinkComponents(dirname(finalPath), true);
    await assertNoSymlinkComponents(finalPath, true);
    await assertNoSymlinkComponents(stagedPath, true);
    if (await pathExists(finalPath)) {
      await verifiedContents(finalPath, file.size, file.sha256);
      continue;
    }
    if (!(await pathExists(stagedPath))) {
      throw new Error(
        `Cannot recover interrupted migration publication: missing ${stagedPath} and ${finalPath}`,
      );
    }
    await mkdir(dirname(finalPath), { recursive: true });
    await assertNoSymlinkComponents(dirname(finalPath));
    await verifiedContents(stagedPath, file.size, file.sha256);
    await publishStagedFileNoReplace(stagedPath, finalPath, file.size, file.sha256);
    await verifiedContents(finalPath, file.size, file.sha256);
    await syncDirectory(dirname(finalPath));
  }
  await rm(journalPath, { force: true });
  await syncDirectory(migrationsDir);
  await rm(join(migrationsDir, MIGRATION_STAGE_DIR), { recursive: true, force: true });
  await syncDirectory(migrationsDir);
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

async function signalMigrationLockContentionForTest(): Promise<void> {
  if (process.env.NODE_ENV !== "test") return;
  const marker = process.env.JAZZ_TEST_MIGRATION_LOCK_CONTENTION_MARKER;
  if (marker) await writeFile(marker, "contended");
}

/**
 * Publish one logical migration generation. Multiple renames cannot be atomic,
 * so a crash may briefly expose a prefix of the files. The fsynced journal is
 * written first; every later invocation recovers that exact generation under
 * the directory lock before inspecting baselines or allocating a timestamp.
 */
async function publishMigrationFilesRecoverably(
  storage: MigrationStorage,
  files: MigrationPublicationFile[],
): Promise<void> {
  if (files.length === 0) return;
  const migrationsDir = storage.migrationsDir;
  await assertNoSymlinkComponents(migrationsDir);
  const stageDir = join(migrationsDir, MIGRATION_STAGE_DIR);
  const journalPath = join(migrationsDir, MIGRATION_JOURNAL);
  await assertNoSymlinkComponents(journalPath, true);
  await mkdir(stageDir, { recursive: true });
  await assertNoSymlinkComponents(stageDir);
  const journal: MigrationPublicationJournal = { version: 2, files: [] };
  for (const [index, file] of files.entries()) {
    await assertNoSymlinkComponents(dirname(file.finalPath), true);
    await assertNoSymlinkComponents(file.finalPath, true);
    if (await pathExists(file.finalPath)) {
      throw new Error(`Migration output already exists: ${file.finalPath}`);
    }
    const stagedName = `${randomUUID()}-${index}`;
    const stagedPath = join(stageDir, stagedName);
    await syncFile(stagedPath, file.contents);
    const identity = contentIdentity(file.contents);
    await verifiedContents(stagedPath, identity.size, identity.sha256);
    journal.files.push({
      stagedName,
      finalRelativePath: relativePublicationPath(migrationsDir, file.finalPath),
      ...identity,
    });
  }
  await syncDirectory(stageDir);
  const journalTemp = `${journalPath}.${randomUUID()}.tmp`;
  await syncFile(journalTemp, `${JSON.stringify(journal)}\n`);
  await rename(journalTemp, journalPath);
  await syncDirectory(migrationsDir);
  await pauseMigrationPublicationForTest("journaled");
  for (const [index, file] of journal.files.entries()) {
    const finalPath = resolve(migrationsDir, file.finalRelativePath);
    await assertNoSymlinkComponents(dirname(finalPath), true);
    await mkdir(dirname(finalPath), { recursive: true });
    await assertNoSymlinkComponents(dirname(finalPath));
    const stagedPath = join(stageDir, file.stagedName);
    await verifiedContents(stagedPath, file.size, file.sha256);
    await publishStagedFileNoReplace(stagedPath, finalPath, file.size, file.sha256);
    await verifiedContents(finalPath, file.size, file.sha256);
    await syncDirectory(dirname(finalPath));
    if (index === 0 && journal.files.length > 1) {
      await pauseMigrationPublicationForTest("between-publications");
    }
  }
  await rm(journalPath, { force: true });
  await syncDirectory(migrationsDir);
  await rm(stageDir, { recursive: true, force: true });
}

async function committedSnapshotPublication(
  storage: MigrationStorage,
  schema: ResolvedSchemaInput,
  timestamp: string,
): Promise<MigrationPublicationFile | null> {
  const entries = await listSnapshotEntriesForMigrations(storage);
  if (
    entries.some(
      (entry) => entry.hash === schema.hash && isCommittedSnapshotFileName(entry.fileName),
    )
  ) {
    return null;
  }
  return {
    finalPath: join(storage.snapshotsDir, snapshotFilename(schema.hash, timestamp)),
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
  storage: MigrationStorage,
  hash: string,
  label: string,
  appId: string | undefined,
  serverUrl: string | undefined,
  adminSecret: string | undefined,
): Promise<ResolvedSchemaInput> {
  const local = await resolveLocalHistoricalSchema(storage, hash, label);
  if (local) {
    return { hash: local.hash, schema: local.schema };
  }

  return resolveRemoteHistoricalSchema(
    storage,
    hash,
    label,
    requireAppId(appId),
    requireServerValue(serverUrl, "serverUrl"),
    requireServerValue(adminSecret, "adminSecret"),
  );
}

async function createMigrationUnlocked(
  options: CreateMigrationOptions,
  storage: MigrationStorage,
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
  const timestamp = await nextCommittedSnapshotTimestamp(storage);

  if (explicitHashFlow) {
    if (options.fromHash) {
      fromSchema = await resolveHistoricalSchemaForCreateMigration(
        storage,
        options.fromHash,
        "fromHash",
        options.appId,
        options.serverUrl,
        options.adminSecret,
      );
    } else {
      const latest = await loadLatestCommittedSnapshot(storage);
      if (!latest) {
        throw new Error(
          "No committed snapshot found. Provide --fromHash or run `jazz-tools migrations create` once to create an initial snapshot.",
        );
      }
      fromSchema = latest;
    }

    toSchema = options.toHash
      ? await resolveHistoricalSchemaForCreateMigration(
          storage,
          options.toHash,
          "toHash",
          options.appId,
          options.serverUrl,
          options.adminSecret,
        )
      : currentSchema!;
    shouldWriteCommittedSnapshot = !options.toHash;
  } else {
    const latest = await loadLatestCommittedSnapshot(storage);
    if (!latest) {
      const snapshot = await committedSnapshotPublication(storage, currentSchema!, timestamp);
      if (!snapshot) throw new Error("Initial committed snapshot already exists");
      await publishMigrationFilesRecoverably(storage, [snapshot]);
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
      ? await committedSnapshotPublication(storage, toSchema, timestamp)
      : null;
    if (snapshot) await publishMigrationFilesRecoverably(storage, [snapshot]);
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
    ? await committedSnapshotPublication(storage, toSchema, timestamp)
    : null;
  await publishMigrationFilesRecoverably(storage, [
    { finalPath: filePath, contents: stub },
    ...(snapshot ? [snapshot] : []),
  ]);

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
  storage: MigrationStorage,
  operation: () => Promise<T>,
): Promise<T> {
  const migrationsDir = storage.migrationsDir;
  await assertNoSymlinkComponents(migrationsDir);
  const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
  const ownerPath = join(lockDir, "owner.json");
  const owner: MigrationLockOwner = {
    version: 1,
    pid: process.pid,
    hostname: hostname(),
    token: randomUUID(),
  };
  const deadline = Date.now() + 10_000;
  let unknownOwnerSince: number | null = null;
  for (;;) {
    try {
      // mkdir is the no-replace cross-process compare-and-set. A process that
      // observes the short owner-write window fails closed rather than ever
      // treating an unknown lock as stale.
      await mkdir(lockDir);
      await syncFile(ownerPath, `${JSON.stringify(owner)}\n`);
      await syncDirectory(lockDir);
      await syncDirectory(migrationsDir);
      await pauseMigrationPublicationForTest("lock-held");
      break;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST") throw error;
      try {
        await assertNoSymlinkComponents(lockDir);
      } catch (validationError) {
        if ((validationError as NodeJS.ErrnoException).code === "ENOENT") continue;
        throw validationError;
      }
      let existing: MigrationLockOwner | null = null;
      let existingText = "";
      try {
        await assertNoSymlinkComponents(ownerPath);
        existingText = await readFile(ownerPath, "utf8");
        existing = parseMigrationLockOwner(existingText);
      } catch {
        if (!(await pathExists(lockDir))) continue;
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
      unknownOwnerSince = null;
      await signalMigrationLockContentionForTest();
      if (
        existing?.version === 1 &&
        existing.hostname === hostname() &&
        Number.isSafeInteger(existing.pid) &&
        existing.pid > 0 &&
        !processIsAlive(existing.pid)
      ) {
        const quarantine = `${lockDir}.recovery-${owner.token}`;
        try {
          await rename(lockDir, quarantine);
        } catch (quarantineError) {
          if ((quarantineError as NodeJS.ErrnoException).code === "ENOENT") continue;
          throw quarantineError;
        }
        await assertNoSymlinkComponents(quarantine);
        await assertNoSymlinkComponents(join(quarantine, "owner.json"));
        const quarantinedOwner = await readFile(join(quarantine, "owner.json"), "utf8");
        if (quarantinedOwner !== existingText) {
          throw new Error(`Quarantined migration lock owner did not match: ${quarantine}`);
        }
        await pauseMigrationPublicationForTest("lock-quarantined");
        await rm(quarantine, { recursive: true });
        await syncDirectory(migrationsDir);
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
    await recoverMigrationPublication(storage);
    return await operation();
  } finally {
    const current = await readFile(ownerPath, "utf8").catch(() => "");
    if (current === `${JSON.stringify(owner)}\n`) {
      await rm(lockDir, { recursive: true, force: true });
      await syncDirectory(migrationsDir);
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
  const storage = new MigrationStorage(options.migrationsDir);
  await storage.initialize();
  return withMigrationDirectoryLock(storage, () => createMigrationUnlocked(options, storage));
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

async function bundleToPrivateTempFile(
  filePath: string,
): Promise<{ outFile: string; tempDir: string }> {
  // A migration is executable local source.  Keep the same project-storage
  // boundary as snapshots and publication journals: do not follow a replaced
  // migration entry while preparing the private bundle.
  await assertNoSymlinkComponents(filePath);
  const sourceDir = dirname(resolve(filePath));
  const tempDir = await mkdtemp(join(sourceDir, ".jazz-bundle-"));
  const outFile = join(tempDir, "migration.mjs");

  try {
    await build({
      entryPoints: [resolve(filePath)],
      bundle: true,
      format: "esm",
      platform: "node",
      outfile: outFile,
      packages: "external",
    });
    return { outFile, tempDir };
  } catch (error) {
    await rm(tempDir, { recursive: true, force: true });
    throw error;
  }
}

async function loadDefinedMigration(filePath: string): Promise<DefinedMigration> {
  const { outFile, tempDir } = await bundleToPrivateTempFile(filePath);
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
    await rm(tempDir, { recursive: true, force: true });
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
  storage: MigrationStorage,
  hash: string,
  label: string,
): Promise<ResolvedSchemaInput | null> {
  const localEntry = await storage.resolveSnapshot(hash, label);
  if (!localEntry) {
    return null;
  }

  return {
    hash: localEntry.hash,
    schema: localEntry.schema,
  };
}

async function resolveRemoteHistoricalSchema(
  storage: MigrationStorage,
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
      storage,
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

/**
 * Publishes the migration that connects two schemas.
 *
 * When a reviewed migration file is not present, this publishes an empty migration
 * only if the schema transition does not require row transformations.
 */
export async function pushMigration(options: PushMigrationOptions): Promise<PushMigrationResult> {
  // `push` reads executable migration source as well as the snapshot/create
  // commands' durable files.  Initialize the common storage boundary first so
  // it cannot silently follow a symlinked migrations directory.
  const storage = new MigrationStorage(options.migrationsDir);
  await storage.initialize();
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

  await assertNoSymlinkComponents(migrationsDir);
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
    new MigrationStorage(migrationsDir).listSnapshots(),
  ]);
  const knownHashes = [
    ...new Set([...stored.hashes, ...snapshots.map(({ hash }) => hash), toHash]),
  ];
  const canonicalSchemas = new Map<string, Promise<WasmSchema>>(
    snapshots.map(({ hash, schema }) => [hash, Promise.resolve(schema)]),
  );
  canonicalSchemas.set(toHash, Promise.resolve(compiled.wasmSchema));
  const loadCanonicalSchema = (hash: string): Promise<WasmSchema> => {
    const cached = canonicalSchemas.get(hash);
    if (cached) return cached;
    const loading = fetchStoredWasmSchema(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schemaHash: hash,
    }).then((storedSchema) => storedSchema.schema);
    canonicalSchemas.set(hash, loading);
    return loading;
  };
  const edges = await Promise.all(
    files.map(async (fileName) => {
      const filePath = join(migrationsDir, fileName);
      const migration = await loadDefinedMigration(filePath);
      if (migration.fromHash === undefined || migration.toHash === undefined) {
        throw new Error(
          `Migration ${fileName} must embed fromHash and toHash metadata; regenerate the migration before deployment.`,
        );
      }

      const fromHash = resolveKnownSchemaHash(
        migration.fromHash,
        `embedded fromHash in ${fileName}`,
        knownHashes,
      );
      const toHash = resolveKnownSchemaHash(
        migration.toHash,
        `embedded toHash in ${fileName}`,
        knownHashes,
      );
      const named = migrationHashesFromFileName(fileName);
      const namedFromHash = resolveKnownSchemaHash(
        named.from,
        `fromHash in ${fileName}`,
        knownHashes,
      );
      const namedToHash = resolveKnownSchemaHash(named.to, `toHash in ${fileName}`, knownHashes);
      if (namedFromHash !== fromHash || namedToHash !== toHash) {
        throw new Error(
          `Migration filename ${fileName} does not match its embedded ${shortSchemaHash(fromHash)} -> ${shortSchemaHash(toHash)} edge.`,
        );
      }

      const [fromSchema, toSchema] = await Promise.all([
        loadCanonicalSchema(fromHash),
        loadCanonicalSchema(toHash),
      ]);
      assertMigrationMatchesCanonicalBundle(migration, {
        fromHash,
        toHash,
        fromSchema,
        toSchema,
      });

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
    const snapshots = await new MigrationStorage(migrationsDir).listSnapshots();

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

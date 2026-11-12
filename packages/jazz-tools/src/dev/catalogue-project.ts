import { validateSchemaAndPermissions } from "./schema-validation.js";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
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
  rmdir,
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
import {
  CLIENT_MIGRATIONS_MODULE,
  clientMigrationsPath,
  hasRootSchema,
  loadCompiledSchema,
  importsClientMigrations,
  rootSchemaFile,
  writeClientMigrationsModule,
  type LoadedSchemaProject,
} from "../schema-loader.js";
import {
  collectMissingExplicitPolicyDiagnostics,
  mergePermissionsIntoWasmSchema,
} from "../schema-permissions.js";
import { collectConventionalProvenanceDiagnostics } from "../provenance-guidance.js";
import { fetchSchemaHashes, fetchStoredWasmSchema } from "./catalogue-api.js";
import { renderMigrationStub, UNFILLED_STUB_TODO } from "./migrations.js";
import { normalizeSchemaHashInput } from "./schema-utils.js";
import { legacySchemaHash } from "./legacy-schema-hash.js";
import {
  assertMigrationMatchesCanonicalBundle,
  computeSchemaHash,
  deployArtifacts,
  serializeForwardLenses,
  type DeployResult as CatalogueDeployResult,
  resolveKnownSchemaHash,
  schemaTransitionRequiresRowTransform,
  shortSchemaHash,
} from "./catalogue.js";

import {
  combineMigrationGraphs,
  fetchMigrationGraph,
  type LocalMigrationGraph,
  type MigrationGraph,
} from "./migration-graph.js";

export { shortSchemaHash };

export type CatalogueEvent =
  | { type: "schema-loaded"; schemaFile?: string }
  | { type: "schema-published"; hash: string; objectId?: string }
  | { type: "schema-skipped"; hash: string; reason: "already-stored" }
  | { type: "permissions-loaded"; permissionsFile?: string }
  | { type: "permissions-published"; schemaHash: string; version?: number }
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

export interface DeploySchemaResult {
  hash: string;
  schemaFile: string;
  status: "published" | "already-stored";
}

export interface DeployResult extends CatalogueDeployResult {
  schema: DeploySchemaResult;
}

export interface DeployOptions extends CatalogueProjectOptions {
  /**
   * Directory containing migration files. Defaults to `<schemaDir>/migrations`.
   */
  migrationsDir?: string;
  /**
   * Confirms switching tables of the active schema to latest-only history,
   * which drops their past versions for good.
   */
  allowDroppingHistory?: boolean;
}

export interface ValidateProjectOptions {
  schemaDir: string;
  migrationsDir?: string;
  /** Move, rename and write nothing; fail if `validate` would move or rename a migration or snapshot. */
  check?: boolean;
}

export interface ValidateProjectResult {
  schemaFile: string;
  permissionsFile?: string;
  tableCount: number;
  warnings: string[];
  /**
   * How schema.ts relates to the project's committed snapshots:
   * `snapshotted` when one is of it, `changed` when it differs from every
   * one (a schema change that needs `migrations create`), `unsnapshotted`
   * when the project has none yet.
   */
  schema: {
    hash: string;
    status: "snapshotted" | "changed" | "unsnapshotted";
  };
}

interface CompileSchemaOptions {
  schemaDir: string;
  migrationsDir?: string;
}

interface CompileSchemaResult {
  schema: WasmSchema;
  hash: string;
  snapshotPath: string | null;
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
      status: "unchanged";
    }
  | {
      /** The project had no snapshot: the current schema became its first. */
      status: "initial-snapshot";
      hash: string;
      snapshotPath: string;
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
      /** The steps the stub leaves to fill in (its `// TODO`s). */
      unfilledSteps: number;
      snapshotPath: string | null;
    };

function emit(options: { onEvent?: (event: CatalogueEvent) => void }, event: CatalogueEvent): void {
  options.onEvent?.(event);
}

function ensurePermissionsProject(compiled: LoadedSchemaProject): LoadedSchemaProject & {
  permissions: NonNullable<LoadedSchemaProject["permissions"]>;
  permissionsFile: string;
} {
  if (
    !compiled.permissions ||
    compiled.permissionsFile !== join(compiled.rootDir, "permissions.ts")
  ) {
    throw new Error(
      "Missing permissions.ts for this app. Create a permissions.ts file before validating or deploying.",
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
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  await validateSchemaAndPermissions(compiled.wasmSchema, compiled.permissions);
  const migrationsDir = resolvedMigrationsDir(options.schemaDir, options.migrationsDir);
  await refuseUnfilledStubs(migrationsDir);
  // Legacy migrations go first: alpha.59 wrote them in a DSL this version
  // no longer has, so loading them (as the check below does) would fail.
  let retired: string[] = [];
  if (options.check) {
    await checkLegacyMigrations(migrationsDir, compiled.wasmSchema);
  } else {
    retired = await retireLegacyMigrations(migrationsDir, compiled.wasmSchema);
  }
  await checkLocalMigrations(compiled.wasmSchema, migrationsDir);
  const schema = await schemaSnapshotStatus(migrationsDir, compiled.wasmSchema);
  if (!options.check) await writeLocalClientMigrations(options.schemaDir, migrationsDir);
  const unshipped = await unshippedMigrationsWarning(options.schemaDir, migrationsDir);
  return {
    schema,
    schemaFile: compiled.schemaFile,
    permissionsFile: compiled.permissionsFile,
    tableCount: compiled.schema.tables.length,
    warnings: [
      ...(unshipped ? [unshipped] : []),
      ...retired,
      ...collectConventionalProvenanceDiagnostics(compiled.schema),
      ...collectMissingExplicitPolicyDiagnostics(
        compiled.schema.tables.map((table) => table.name),
        compiled.permissions,
      ).map((diagnostic) => diagnostic.message),
    ],
  };
}

/** A line that gives `column` a step: `column: …` or `"column": …`. */
function stepKey(column: string): RegExp {
  const name = column.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`^\\s*(?:${name}|"${name}"|'${name}')\\s*:`);
}

/**
 * The lines after `lines[index]` up to the end of the block it sits in: the
 * first later line indented less closes it. A stub's TODO sits in its
 * table's `migrate` block, so this stops before the next table and before
 * the `from`/`to` witnesses, which name every column.
 */
function tableBlockAfter(lines: readonly string[], index: number): readonly string[] {
  const indent = (line: string) => line.length - line.trimStart().length;
  const own = indent(lines[index]!);
  const block: string[] = [];
  for (const line of lines.slice(index + 1)) {
    if (line.trim() !== "" && indent(line) < own) break;
    block.push(line);
  }
  return block;
}

/** Refuse a migration file that still holds a step its stub left to fill in. */
async function refuseUnfilledStubs(migrationsDir: string): Promise<void> {
  const files = await readdir(migrationsDir).catch(() => [] as string[]);
  const unfilled: string[] = [];
  for (const file of files.filter((name) => name.endsWith(".ts")).sort()) {
    const lines = (await readFile(join(migrationsDir, file), "utf8")).split("\n");
    lines.forEach((line, index) => {
      const todo = UNFILLED_STUB_TODO.exec(line);
      // Authors often fill the step in under the comment and keep it, so
      // only a column its table's block gives no step is unfilled.
      if (todo && !tableBlockAfter(lines, index).some((later) => stepKey(todo[2]!).test(later))) {
        unfilled.push(`  ${join(basename(migrationsDir), file)}:${index + 1}: ${todo[1]}`);
      }
    });
  }
  if (unfilled.length === 0) return;
  throw new Error(
    `A migration still has steps to fill in, so it cannot deploy:\n${unfilled.join("\n")}\n` +
      "Replace each TODO with its step (for example `points: m.add.int({ default: 1 })`).",
  );
}

/** How `schema` relates to the committed snapshots in `migrationsDir`. */
async function schemaSnapshotStatus(
  migrationsDir: string,
  schema: WasmSchema,
): Promise<ValidateProjectResult["schema"]> {
  const hash = await computeSchemaHash(schema);
  const committed = (await new MigrationStorage(migrationsDir).listSnapshots()).filter((entry) =>
    isCommittedSnapshotFileName(entry.fileName),
  );
  if (committed.some((entry) => entry.hash === hash)) return { hash, status: "snapshotted" };
  return { hash, status: committed.length === 0 ? "unsnapshotted" : "changed" };
}

export async function compileSchema(options: CompileSchemaOptions): Promise<CompileSchemaResult> {
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
    const dir = this.snapshotsDir;
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

  async resolveSnapshot(hash: string, label: string): Promise<SnapshotEntry | null> {
    const entries = await this.listSnapshots();
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

  async writeSnapshot(fileName: string, schema: WasmSchema): Promise<string> {
    if (basename(fileName) !== fileName || !looksLikeSnapshotFileName(fileName)) {
      throw new Error(`Invalid snapshot filename: ${fileName}`);
    }
    await assertNoSymlinkComponents(this.snapshotsDir, true);
    await mkdir(this.snapshotsDir, { recursive: true });
    await assertNoSymlinkComponents(this.snapshotsDir);
    const filePath = join(this.snapshotsDir, fileName);
    await assertNoSymlinkComponents(filePath, true);
    const contents = `${JSON.stringify(schema, runtimeSchemaJsonReplacer, 2)}\n`;
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
  const latest = (await storage.listSnapshots())
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

function resolvedMigrationsDir(schemaDir: string, migrationsDir?: string): string {
  return migrationsDir ?? join(schemaDir, "migrations");
}

async function listLocalSnapshotEntries(
  schemaDir: string,
  migrationsDir?: string,
): Promise<SnapshotEntry[]> {
  const storage = new MigrationStorage(resolvedMigrationsDir(schemaDir, migrationsDir));
  await storage.initialize();
  return storage.listSnapshots();
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

export function requireServerValue(
  value: string | undefined,
  kind: "serverUrl" | "adminSecret",
): string {
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

export function requireAppId(appId: string | undefined): string {
  if (appId) {
    return appId;
  }

  throw new Error(
    "Missing app ID. Pass an <appId> positional argument or set JAZZ_APP_ID (or a framework-prefixed form such as VITE_JAZZ_APP_ID).",
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
  storage: MigrationStorage,
): Promise<ResolvedSchemaInput | null> {
  const entries = await storage.listSnapshots();
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
  version: 1;
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
  if (journal.version !== 1 || !Array.isArray(journal.files) || journal.files.length === 0) {
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

function failMigrationStepForTest(step: string): void {
  if (process.env.NODE_ENV !== "test" || process.env.JAZZ_TEST_MIGRATION_FAIL_AT !== step) return;
  throw Object.assign(new Error(`EBUSY: injected test failure at ${step}`), { code: "EBUSY" });
}

function migrationLockTimeoutMs(): number {
  const override = Number(process.env.JAZZ_TEST_MIGRATION_LOCK_TIMEOUT_MS);
  return process.env.NODE_ENV === "test" && Number.isSafeInteger(override) && override > 0
    ? override
    : 10_000;
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
  const journal: MigrationPublicationJournal = { version: 1, files: [] };
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
  const entries = await storage.listSnapshots();
  if (
    entries.some(
      (entry) => entry.hash === schema.hash && isCommittedSnapshotFileName(entry.fileName),
    )
  ) {
    return null;
  }
  return {
    finalPath: join(storage.snapshotsDir, snapshotFilename(schema.hash, timestamp)),
    contents: `${JSON.stringify(schema.schema, runtimeSchemaJsonReplacer, 2)}\n`,
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
  const local = await storage.resolveSnapshot(hash, label);
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

const NO_BASELINE =
  "No schema snapshot in migrations/snapshots to diff against. Run `jazz-tools migrations create` " +
  "without --fromHash/--toHash on the schema.ts your backend runs to snapshot it as the first " +
  "version. With Jazz Cloud (`remote()`), --server-url and --admin-secret use the schema of the " +
  "app's latest bundle instead.";

async function loadMigrationBaseline(
  options: CreateMigrationOptions,
  storage: MigrationStorage,
): Promise<ResolvedSchemaInput> {
  const local = await loadLatestCommittedSnapshot(storage);
  if (local) return local;
  if (!options.serverUrl) {
    throw new Error(NO_BASELINE);
  }
  const appId = requireAppId(options.appId);
  const adminSecret = requireServerValue(options.adminSecret, "adminSecret");
  const graph = await fetchMigrationGraph({ appId, serverUrl: options.serverUrl, adminSecret });
  if (!graph.latestBundleSchemaHash) {
    throw new Error(
      "No local schema snapshot or published bundle found. Deploy the initial schema before creating a migration. No snapshot was created.",
    );
  }
  return resolveRemoteHistoricalSchema(
    storage,
    graph.latestBundleSchemaHash,
    "latest bundle's schema",
    appId,
    options.serverUrl,
    adminSecret,
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
      fromSchema = await loadMigrationBaseline(options, storage);
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
    if (!options.serverUrl && !(await loadLatestCommittedSnapshot(storage))) {
      // The project's first snapshot: there is nothing to diff against yet.
      const publication = await committedSnapshotPublication(
        storage,
        currentSchema!,
        await nextCommittedSnapshotTimestamp(storage),
      );
      if (!publication) throw new Error(NO_BASELINE);
      await publishMigrationFilesRecoverably(storage, [publication]);
      return {
        status: "initial-snapshot",
        hash: currentSchema!.hash,
        snapshotPath: publication.finalPath,
      };
    }
    fromSchema = await loadMigrationBaseline(options, storage);
    toSchema = currentSchema!;
    shouldWriteCommittedSnapshot = true;
  }

  // Snapshot filenames are the ordering source for the implicit migration
  // baseline. Treat their second-resolution timestamp as a logical clock so
  // rapid successive schema edits cannot let the hash suffix pick the winner.
  const timestamp = await nextCommittedSnapshotTimestamp(storage);

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
    unfilledSteps: stub.split("\n").filter((line) => UNFILLED_STUB_TODO.test(line)).length,
    snapshotPath: snapshot?.finalPath ?? null,
  };
}

const MIGRATION_LOCK_OWNER_RECORD =
  /^owner-([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.json$/i;

function migrationLockOwnerRecordName(token: string): string {
  return `owner-${token}.json`;
}

/**
 * Serialize migration generation across processes.
 *
 * `mkdir(lockDir)` is the no-replace compare-and-set that acquires the lock.
 * Each lock instance holds exactly one owner record whose file name carries
 * that instance's random token, so a path to the record identifies one lock
 * instance, not whichever lock currently sits at `lockDir`. Release and
 * stale-lock recovery both start by removing that token-named record, which
 * fails with ENOENT if the instance they mean is already gone, so neither can
 * ever remove a lock that another generator acquired later. An ownerless lock
 * directory is never removed by anyone else, so the following rmdir is safe.
 */
async function withMigrationDirectoryLock<T>(
  storage: MigrationStorage,
  operation: () => Promise<T>,
): Promise<T> {
  const migrationsDir = storage.migrationsDir;
  await assertNoSymlinkComponents(migrationsDir);
  const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
  const owner: MigrationLockOwner = {
    version: 1,
    pid: process.pid,
    hostname: hostname(),
    token: randomUUID(),
  };
  const ownerPath = join(lockDir, migrationLockOwnerRecordName(owner.token));
  const deadline = Date.now() + migrationLockTimeoutMs();
  const staleLockHint = `If no \`jazz-tools migrations create\` is running for this directory, delete ${lockDir} and retry.`;
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
      let existing: MigrationLockOwner;
      let existingPath: string;
      let legacyOwner = false;
      try {
        const entries = await readdir(lockDir);
        legacyOwner = entries.includes("owner.json");
        const [record, ...extra] = entries;
        const recordToken = record ? MIGRATION_LOCK_OWNER_RECORD.exec(record)?.[1] : undefined;
        if (!record || !recordToken || extra.length > 0) {
          throw new Error("expected exactly one owner record");
        }
        existingPath = join(lockDir, record);
        await assertNoSymlinkComponents(existingPath);
        existing = parseMigrationLockOwner(await readFile(existingPath, "utf8"));
        if (existing.token !== recordToken) {
          throw new Error("owner record name does not match its token");
        }
      } catch {
        if (!(await pathExists(lockDir))) continue;
        unknownOwnerSince ??= Date.now();
        // A freshly created lock has a necessarily brief mkdir→owner-write
        // window. Wait for that owner, but never steal the lock if it remains
        // unknown: fail closed with the directory intact.
        if (Date.now() - unknownOwnerSince >= 500) {
          const legacy = legacyOwner
            ? " (it holds an owner.json left by an older jazz-tools, which is never recovered automatically)"
            : "";
          throw new Error(
            `Cannot safely acquire migration lock ${lockDir}; owner metadata is missing, invalid, or unsafe${legacy}. ${staleLockHint}`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
        continue;
      }
      unknownOwnerSince = null;
      await signalMigrationLockContentionForTest();
      await pauseMigrationPublicationForTest("lock-observed");
      if (existing.hostname === hostname() && !processIsAlive(existing.pid)) {
        // The lock we observed may have been released and replaced by a live
        // generator since we read it. Claim the observed instance by its
        // token-named record, never by the shared lock path: if that instance
        // is gone, this rename fails and we re-inspect whatever is there now.
        const claimed = `${lockDir}.recovery-${owner.token}.json`;
        try {
          await rename(existingPath, claimed);
        } catch (claimError) {
          if ((claimError as NodeJS.ErrnoException).code === "ENOENT") continue;
          throw claimError;
        }
        // The claimed instance is now ownerless. Its owner is dead and every
        // other recoverer's claim fails with ENOENT, so nobody else can remove
        // it and no generator can mkdir over it: lockDir is still exactly the
        // instance we observed, and it is empty.
        try {
          await rmdir(lockDir);
        } catch (removeError) {
          // Something unexpected is inside the lock; hand the record back so
          // the lock stays attributable and fails closed rather than ownerless.
          await rename(claimed, existingPath).catch(() => undefined);
          throw removeError;
        }
        await syncDirectory(migrationsDir);
        await pauseMigrationPublicationForTest("lock-recovered");
        await rm(claimed, { force: true });
        continue;
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `Timed out waiting for another migration generator to release ${lockDir}; it is held by pid ${existing.pid} on host ${existing.hostname}. ${staleLockHint}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }

  try {
    await recoverMigrationPublication(storage);
    return await operation();
  } finally {
    await releaseMigrationDirectoryLock(lockDir, ownerPath, owner.token);
  }
}

/**
 * Release our lock instance the same way recovery claims a stale one: move our
 * token-named record out first, so the directory becomes an ownerless lock that
 * nobody else removes and the rmdir can only hit our own instance. If the
 * rmdir fails (for example EBUSY from a Windows virus scanner), put the record
 * back: the lock then stays attributed to our pid, which is dead once we exit,
 * so the next run recovers it instead of failing closed on an ownerless lock.
 */
async function releaseMigrationDirectoryLock(
  lockDir: string,
  ownerPath: string,
  token: string,
): Promise<void> {
  const released = `${lockDir}.released-${token}.json`;
  try {
    await rename(ownerPath, released);
  } catch {
    // Our record is already gone, so this instance is no longer ours to remove.
    return;
  }
  try {
    failMigrationStepForTest("release-rmdir");
    await rmdir(lockDir);
  } catch (error) {
    await rename(released, ownerPath).catch(() => undefined);
    console.warn(
      `Could not remove migration lock ${lockDir} (${(error as NodeJS.ErrnoException).code ?? error}); the next run will recover it.`,
    );
    return;
  }
  await unlink(released).catch(() => undefined);
  await syncDirectory(dirname(lockDir));
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
  const result = await withMigrationDirectoryLock(storage, async () =>
    createMigrationUnlocked(options, storage),
  );
  // The app ships migrations/index.ts: what this wrote belongs in it now,
  // not only after the next `validate`.
  if (result.status !== "unchanged" && (await hasRootSchema(options.schemaDir))) {
    await writeLocalClientMigrations(options.schemaDir, options.migrationsDir);
  }
  return result;
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

// Migration builders that moved from `schema` (or the removed `col` export) to `migration`.
const LEGACY_MIGRATION_DSL_PATTERN =
  /\b(?:s|schema|col)\.(?:add|drop|renameFrom|defineMigration|renameTableFrom)\b|\bimport\s*\{[^}]*\bcol\b[^}]*\}\s*from\s*["']jazz-tools["']/;

export async function loadDefinedMigration(filePath: string): Promise<DefinedMigration> {
  const { outFile, tempDir } = await bundleToPrivateTempFile(filePath);
  try {
    let loaded: { default?: unknown; migration?: unknown };
    try {
      loaded = await import(pathToFileURL(outFile).href);
    } catch (error) {
      const source = await readFile(filePath, "utf8");
      const hint = LEGACY_MIGRATION_DSL_PATTERN.test(source)
        ? " The migration DSL moved to `migration as m` from jazz-tools: use m.defineMigration, m.add, m.drop, m.renameFrom and m.renameTableFrom, and s.* for schema columns."
        : "";
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to load migration ${basename(filePath)}: ${reason}.${hint}`, {
        cause: error,
      });
    }
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
    await storage.writeSnapshot(
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

function migrationHashesFromFileName(fileName: string): { from: string; to: string } {
  const match = fileName.match(/-([0-9a-f]{12,64})-([0-9a-f]{12,64})\.ts$/i);
  if (!match) {
    throw new Error(
      `Migration filename ${fileName} must end in -<fromHash>-<toHash>.ts using at least 12 hex characters per hash.`,
    );
  }
  return { from: match[1]!, to: match[2]! };
}

export interface MigrationGraphOptions extends Omit<CatalogueProjectOptions, "onEvent"> {
  migrationsDir?: string;
}

/** Load and compare local and server history without publishing any artifacts. */
export async function getMigrationGraph(options: MigrationGraphOptions): Promise<MigrationGraph> {
  const server = await fetchMigrationGraph(options);
  const local = await loadLocalMigrationGraph(
    options.schemaDir,
    resolvedMigrationsDir(options.schemaDir, options.migrationsDir),
    server.schemas,
  );
  return combineMigrationGraphs(server, local);
}

/** Read local history without creating snapshots, migrations, or their directories. */
async function loadLocalMigrationGraph(
  schemaDir: string,
  migrationsDir: string,
  serverSchemaHashes: readonly string[],
): Promise<LocalMigrationGraph> {
  const snapshots = await new MigrationStorage(migrationsDir).listSnapshots();
  const current = (await hasRootSchema(schemaDir)) ? await loadCurrentSchema(schemaDir) : null;
  const schemas = [
    ...new Set([...snapshots.map(({ hash }) => hash), ...(current ? [current.hash] : [])]),
  ];
  const knownHashes = [...new Set([...serverSchemaHashes, ...schemas])];
  const migrations: LocalMigrationGraph["migrations"] = [];
  if (await pathExists(migrationsDir)) {
    await assertNoSymlinkComponents(migrationsDir);
    for (const fileName of (await readdir(migrationsDir)).filter(isMigrationFile).sort()) {
      const migration = await loadDefinedMigration(join(migrationsDir, fileName));
      if (!migration.fromHash || !migration.toHash) {
        throw new Error(
          `Migration ${fileName} must embed fromHash and toHash metadata; regenerate it to include it in the graph.`,
        );
      }
      const resolveHash = (hash: string, label: string) => {
        try {
          return resolveKnownSchemaHash(hash, label, knownHashes);
        } catch (error) {
          throw new Error(
            `Cannot resolve ${label} in ${fileName} against local snapshots, schema.ts, or server schemas: ${(error as Error).message}`,
          );
        }
      };
      const fromHash = resolveHash(migration.fromHash, "fromHash");
      const toHash = resolveHash(migration.toHash, "toHash");
      const named = migrationHashesFromFileName(fileName);
      if (
        resolveHash(named.from, "filename fromHash") !== fromHash ||
        resolveHash(named.to, "filename toHash") !== toHash
      ) {
        throw new Error(
          `Migration filename ${fileName} does not match its embedded schema hashes.`,
        );
      }
      migrations.push({ fromHash, toHash });
    }
  }
  return { schemas, migrations, currentSchemaHash: current?.hash ?? null };
}

/** Publish all missing local history and schema.ts with its permissions as a bundle. */
export async function deploy(options: DeployOptions): Promise<DeployResult> {
  const migrationsDir = resolvedMigrationsDir(options.schemaDir, options.migrationsDir);
  if ("noVerify" in options)
    throw new Error("noVerify is no longer supported; deploy requires a complete migration path.");
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  await validateSchemaAndPermissions(compiled.wasmSchema, compiled.permissions);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });
  emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });
  const toHash = await computeSchemaHash(compiled.wasmSchema);
  await assertNoLegacyMigrations(migrationsDir, compiled.wasmSchema);
  const graph = await fetchMigrationGraph(options);
  if (await pathExists(migrationsDir)) await assertNoSymlinkComponents(migrationsDir);
  const files = ((await pathExists(migrationsDir)) ? await readdir(migrationsDir) : [])
    .filter(isMigrationFile)
    .sort();
  const snapshots = await new MigrationStorage(migrationsDir).listSnapshots();
  const knownHashes = [
    ...new Set([...graph.schemas, ...snapshots.map(({ hash }) => hash), toHash]),
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

  const schemas = new Map(snapshots.map(({ hash, schema }) => [hash, schema]));
  schemas.set(toHash, compiled.wasmSchema);
  const result: CatalogueDeployResult = await deployArtifacts(options, graph, {
    targetSchemaHash: toHash,
    schemas: [...schemas].map(([hash, schema]) => ({
      hash,
      schema: mergePermissionsIntoWasmSchema(schema, {}),
    })),
    migrations: edges.map((edge) => ({
      fromHash: edge.fromHash,
      toHash: edge.toHash,
      forward: serializeForwardLenses(edge.migration.forward),
    })),
    permissions: compiled.permissions,
  });
  for (const warning of result.warnings) emit(options, { type: "warning", message: warning });
  for (const hash of result.published.schemas) emit(options, { type: "schema-published", hash });
  if (result.schema.status === "already-stored")
    emit(options, { type: "schema-skipped", hash: toHash, reason: "already-stored" });
  const storedEdges = new Set(
    graph.migrations.map(({ fromHash, toHash }) => `${fromHash}:${toHash}`),
  );
  for (const { fromHash, toHash } of edges) {
    if (storedEdges.has(`${fromHash}:${toHash}`)) {
      emit(options, { type: "migration-skipped", reason: "already-connected", fromHash, toHash });
    }
  }
  for (const migration of result.published.migrations) {
    emit(options, {
      type: "migration-published",
      ...migration,
      filePath: edges.find(
        (edge) => edge.fromHash === migration.fromHash && edge.toHash === migration.toHash,
      )?.filePath,
    });
  }
  if (result.changed) emit(options, { type: "permissions-published", schemaHash: toHash });
  // Clients ship what the server now holds, so they store each column where
  // it does, even before their first sync (see ClientMigrations).
  const bundled = new Map<string, WasmSchema>();
  for (const hash of [
    ...schemas.keys(),
    ...edges.flatMap((edge) => [edge.fromHash, edge.toHash]),
  ]) {
    if (!bundled.has(hash)) bundled.set(hash, await loadCanonicalSchema(hash));
  }
  await writeClientMigrations(compiled.schemaFile, bundled, edges);
  return { ...result, schema: { ...result.schema, schemaFile: compiled.schemaFile } };
}

/**
 * The module an app imports its `migrations` from, in the `migrations`
 * directory beside schema.ts. For an app whose schema.ts imports it,
 * `jazz-tools` writes it on every validate and `migrations create`, so it
 * exists even while the app has a single schema version.
 */
export const CLIENT_MIGRATIONS_FILE = CLIENT_MIGRATIONS_MODULE;

/** A migration file in a migrations directory (not the generated module). */
function isMigrationFile(fileName: string): boolean {
  return fileName.endsWith(".ts") && fileName !== CLIENT_MIGRATIONS_FILE;
}

type MigrationEdge = { fromHash: string; toHash: string; migration: DefinedMigration };

/** Write the app's schema versions and the migrations between them. */
async function writeClientMigrations(
  schemaFile: string,
  schemas: ReadonlyMap<string, WasmSchema>,
  edges: ReadonlyArray<MigrationEdge>,
): Promise<void> {
  if (!(await importsClientMigrations(schemaFile))) return;
  const path = clientMigrationsPath(schemaFile);
  await assertNoSymlinkComponents(dirname(path), true);
  await writeClientMigrationsModule(path, clientMigrationsJson(schemas, edges));
}

/** The app's schema versions and migrations as JSON, as it ships them to clients. */
function clientMigrationsJson(
  schemas: ReadonlyMap<string, WasmSchema>,
  edges: ReadonlyArray<MigrationEdge>,
): string {
  const bundle = {
    // As a deployment sends them (`{ tables }`), which the bindings decode.
    schemas: [...schemas]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([hash, schema]) => ({
        hash,
        schema: { tables: mergePermissionsIntoWasmSchema(schema, {}) },
      })),
    migrations: edges.map((edge) => ({
      fromHash: edge.fromHash,
      toHash: edge.toHash,
      forward: serializeForwardLenses(edge.migration.forward),
    })),
  };
  return JSON.stringify(bundle, runtimeSchemaJsonReplacer, 2);
}

/** Where `validate` moves migrations that predate SHA-256 schema hashes. */
export const LEGACY_MIGRATIONS_DIR = "legacy";

const SNAPSHOT_NAME_HASH = /([0-9a-f]{12})\.json$/i;
const SNAPSHOT_HASH_PREFIX = 12;

const LEGACY_REASON =
  "names schema versions by the BLAKE3 hashes Jazz used before alpha.60 switched schema hashes to SHA-256";

/** What `validate` does to a project an alpha.59 or older tool left. */
interface LegacyPlan {
  /** Migration files to move to `legacy/`. */
  migrations: string[];
  /** Snapshots named by their BLAKE3 hash, to name by their SHA-256 one. */
  snapshots: { from: string; to: string }[];
  /**
   * Snapshots to move to `legacy/snapshots/`: once the legacy migrations
   * are gone, a snapshot no remaining migration reaches is a second root of
   * the history, which the app refuses ("disconnected roots"). The one for
   * schema.ts stays; with none for it, the newest stays, so `migrations
   * create` can still start from the schema the app's data was written in.
   */
  retired: string[];
  /**
   * The snapshot kept when none is for schema.ts and none is reached by a
   * remaining migration: the newest, which `migrations create` must lead
   * from to schema.ts before the app starts.
   */
  startFrom?: string;
}

/**
 * Schema hashes changed from BLAKE3 to SHA-256 in alpha.60. A snapshot's
 * hash is recomputed from its contents, but its file name and every
 * migration written against it still carry the BLAKE3 short hash.
 *
 * A migration is legacy when one of its ends resolves to no SHA-256
 * version this project knows (its snapshots and schema.ts) but is the
 * BLAKE3 hash of one of them: its edges can't be resolved, and an app
 * upgrading to alpha.60 starts again from its current schema. This is
 * decided from the migration and the schemas alone, so a run that stopped
 * halfway (snapshots renamed, migrations not yet moved) is finished by the
 * next. A migration whose ends match nothing at all is not legacy; it is
 * left for the usual checks to report.
 *
 * A snapshot named by neither hash of its contents is refused: it was
 * edited or renamed by hand, and renaming it would hide that.
 */
async function planLegacyMigrations(
  migrationsDir: string,
  currentSchema: WasmSchema,
): Promise<LegacyPlan> {
  const plan: LegacyPlan = { migrations: [], snapshots: [], retired: [] };
  if (!(await pathExists(migrationsDir))) return plan;
  await assertNoSymlinkComponents(migrationsDir);
  const snapshots = await new MigrationStorage(migrationsDir).listSnapshots();
  const known = [
    ...new Set([...snapshots.map(({ hash }) => hash), await computeSchemaHash(currentSchema)]),
  ];
  const legacyShort = new Set([shortSchemaHash(legacySchemaHash(currentSchema))]);
  for (const snapshot of snapshots) {
    const legacy = shortSchemaHash(legacySchemaHash(snapshot.schema));
    legacyShort.add(legacy);
    const named = snapshot.fileName.match(SNAPSHOT_NAME_HASH)?.[1]?.toLowerCase();
    const current = shortSchemaHash(snapshot.hash);
    if (!named || named === current) continue;
    if (named !== legacy) {
      throw new Error(
        `Snapshot ${snapshot.fileName} is named for schema hash ${named}, but its contents hash to ${current} (and were ${legacy} before alpha.60). Restore the snapshot from version control, or rename it to end in ${current}.json if its contents are right.`,
      );
    }
    plan.snapshots.push({
      from: snapshot.filePath,
      to: join(
        dirname(snapshot.filePath),
        snapshot.fileName.replace(SNAPSHOT_NAME_HASH, `${current}.json`),
      ),
    });
  }
  const resolves = (hash: string) => {
    try {
      resolveKnownSchemaHash(hash, "schema hash", known);
      return true;
    } catch {
      return false;
    }
  };
  const legacy = (hash: string) =>
    !resolves(hash) && legacyShort.has(hash.slice(0, SNAPSHOT_HASH_PREFIX).toLowerCase());
  for (const fileName of (await readdir(migrationsDir)).filter(isMigrationFile).sort()) {
    let named;
    try {
      named = migrationHashesFromFileName(fileName);
    } catch {
      continue;
    }
    if (legacy(named.from) || legacy(named.to)) plan.migrations.push(fileName);
  }
  if (plan.migrations.length > 0 || plan.snapshots.length > 0) {
    await retireUnreachedSnapshots(plan, migrationsDir, snapshots, currentSchema);
  }
  return plan;
}

/** Plan the snapshots of a project being retired that nothing reaches (see
 * [`LegacyPlan.retired`]). */
async function retireUnreachedSnapshots(
  plan: LegacyPlan,
  migrationsDir: string,
  snapshots: SnapshotEntry[],
  currentSchema: WasmSchema,
): Promise<void> {
  const current = await computeSchemaHash(currentSchema);
  const reached = new Set<string>();
  const moving = new Set(plan.migrations);
  for (const fileName of (await readdir(migrationsDir)).filter(isMigrationFile)) {
    if (moving.has(fileName)) continue;
    try {
      const { from, to } = migrationHashesFromFileName(fileName);
      reached.add(from.toLowerCase());
      reached.add(to.toLowerCase());
    } catch {
      continue;
    }
  }
  const isReached = (hash: string) =>
    hash === current || [...reached].some((prefix) => hash.startsWith(prefix));
  let unreached = snapshots.filter(({ hash }) => !isReached(hash));
  if (!snapshots.some(({ hash }) => isReached(hash))) {
    // Committed snapshot names start with their timestamp: the last is the
    // newest, as `loadLatestCommittedSnapshot` picks it.
    const newest = unreached
      .filter(({ fileName }) => isCommittedSnapshotFileName(fileName))
      .sort((a, b) => a.fileName.localeCompare(b.fileName))
      .pop();
    if (newest) {
      unreached = unreached.filter((snapshot) => snapshot !== newest);
      const renamed = plan.snapshots.find(({ from }) => from === newest.filePath);
      plan.startFrom = basename(renamed?.to ?? newest.filePath);
    }
  }
  const retired = new Set(unreached.map(({ filePath }) => filePath));
  plan.snapshots = plan.snapshots.filter(({ from }) => !retired.has(from));
  plan.retired = [...retired].sort();
}

async function sameBytes(left: string, right: string): Promise<boolean> {
  const [a, b] = await Promise.all([readFile(left), readFile(right)]);
  return a.equals(b);
}

/**
 * Carry out {@link planLegacyMigrations} under the migrations lock:
 * migrations move to `legacy/` first, then snapshots are renamed, so an
 * interrupted run leaves a project the next run finishes. A migration whose
 * name is taken in `legacy/` by a different file is refused before
 * anything moves. Returns a warning per change.
 */
async function retireLegacyMigrations(
  migrationsDir: string,
  currentSchema: WasmSchema,
): Promise<string[]> {
  if (!(await pathExists(migrationsDir))) return [];
  const storage = new MigrationStorage(migrationsDir);
  return withMigrationDirectoryLock(storage, async () => {
    const plan = await planLegacyMigrations(migrationsDir, currentSchema);
    const legacyDir = join(migrationsDir, LEGACY_MIGRATIONS_DIR);
    const moves = plan.migrations.map((fileName) => ({
      fileName,
      from: join(migrationsDir, fileName),
      to: join(legacyDir, fileName),
    }));
    const kept = new Set<string>();
    for (const move of moves) {
      if (!(await pathExists(move.to))) continue;
      if (!(await sameBytes(move.from, move.to))) {
        throw new Error(
          `Cannot move ${move.fileName} to ${LEGACY_MIGRATIONS_DIR}/: a different ${LEGACY_MIGRATIONS_DIR}/${move.fileName} is already there. Compare the two, keep the one you want in ${LEGACY_MIGRATIONS_DIR}/ and delete the other, then run validate again.`,
        );
      }
      kept.add(move.fileName);
    }
    const legacySnapshots = join(legacyDir, "snapshots");
    const snapshotMoves = plan.retired.map((from) => ({
      fileName: basename(from),
      from,
      to: join(legacySnapshots, basename(from)),
    }));
    for (const move of snapshotMoves) {
      if ((await pathExists(move.to)) && !(await sameBytes(move.from, move.to))) {
        throw new Error(
          `Cannot move snapshot ${move.fileName} to ${LEGACY_MIGRATIONS_DIR}/snapshots/: a different ${LEGACY_MIGRATIONS_DIR}/snapshots/${move.fileName} is already there. Compare the two, keep the one you want there and delete the other, then run validate again.`,
        );
      }
    }
    if (moves.length > 0 || snapshotMoves.length > 0) {
      await mkdir(legacyDir, { recursive: true });
      await assertNoSymlinkComponents(legacyDir);
    }
    if (snapshotMoves.length > 0) {
      await mkdir(legacySnapshots, { recursive: true });
      await assertNoSymlinkComponents(legacySnapshots);
    }
    const runsOn = plan.startFrom ? "" : " The app runs on schema.ts as it is.";
    const warnings: string[] = [];
    for (const move of moves) {
      if (kept.has(move.fileName)) {
        await rm(move.from);
        warnings.push(
          `Removed ${move.fileName}: it ${LEGACY_REASON}, and the same file is already in ${LEGACY_MIGRATIONS_DIR}/.`,
        );
      } else {
        await rename(move.from, move.to);
        warnings.push(
          `Moved ${move.fileName} to ${LEGACY_MIGRATIONS_DIR}/: it ${LEGACY_REASON}, so it no longer applies.${runsOn} Delete ${LEGACY_MIGRATIONS_DIR}/ once you no longer need it, or exclude it from type checking.`,
        );
      }
    }
    for (const move of snapshotMoves) {
      if (await pathExists(move.to)) {
        await rm(move.from);
      } else {
        await rename(move.from, move.to);
      }
      warnings.push(
        `Moved snapshot ${move.fileName} to ${LEGACY_MIGRATIONS_DIR}/snapshots/: no remaining migration reaches it, so it would be a second root of the history.`,
      );
    }
    for (const snapshot of plan.snapshots) {
      const from = basename(snapshot.from);
      const to = basename(snapshot.to);
      if (await pathExists(snapshot.to)) {
        // The same schema (its SHA-256 says so) under its current name.
        await rm(snapshot.from);
        warnings.push(
          `Removed snapshot ${from}: it is named by its pre-alpha.60 BLAKE3 hash, and ${to} holds the same schema.`,
        );
      } else {
        await rename(snapshot.from, snapshot.to);
        warnings.push(
          `Renamed snapshot ${from} to ${to}: snapshots are named by their SHA-256 schema hash since alpha.60.`,
        );
      }
    }
    if (plan.startFrom) {
      warnings.push(
        `Run \`jazz-tools migrations create\` before the app starts: no snapshot is for schema.ts, so the app needs a migration from ${plan.startFrom}, the newest snapshot.`,
      );
    }
    return warnings;
  });
}

/** Refuse while legacy migrations are in place, pointing at `validate`. */
async function assertNoLegacyMigrations(
  migrationsDir: string,
  currentSchema: WasmSchema,
): Promise<void> {
  const { migrations } = await planLegacyMigrations(migrationsDir, currentSchema);
  if (migrations.length === 0) return;
  const one = migrations.length === 1;
  throw new Error(
    `${migrations.join(", ")} ${one ? "names" : "name"} schema versions by the BLAKE3 hashes Jazz used before alpha.60 switched schema hashes to SHA-256. Run \`jazz-tools validate\` to move ${one ? "it" : "them"} to ${LEGACY_MIGRATIONS_DIR}/; the app then runs on schema.ts as it is.`,
  );
}

/**
 * For `validate --check`: fail, changing nothing, while a project holds
 * legacy migrations or snapshots named by their BLAKE3 hash.
 */
async function checkLegacyMigrations(
  migrationsDir: string,
  currentSchema: WasmSchema,
): Promise<void> {
  const plan = await planLegacyMigrations(migrationsDir, currentSchema);
  const pending = [
    ...plan.migrations.map((fileName) => `move ${fileName} to ${LEGACY_MIGRATIONS_DIR}/`),
    ...plan.retired.map(
      (from) => `move snapshot ${basename(from)} to ${LEGACY_MIGRATIONS_DIR}/snapshots/`,
    ),
    ...plan.snapshots.map(({ from, to }) => `rename snapshot ${basename(from)} to ${basename(to)}`),
  ];
  if (pending.length === 0) return;
  throw new Error(
    `This project still uses the schema hashes Jazz used before alpha.60. Run \`jazz-tools validate\` to ${pending.join("; ")}.${plan.startFrom ? ` Then run \`jazz-tools migrations create\`: no snapshot is for schema.ts, so the app needs a migration from ${plan.startFrom}.` : ""}`,
  );
}

/**
 * The warning for a project that has schema snapshots but whose schema.ts
 * doesn't import `./migrations`: `jazz-tools` writes migrations/index.ts only
 * for an app that imports it, so such an app (still on `{ migrations: "none" }`)
 * ships none of its migrations, and nothing else would say so.
 */
export async function unshippedMigrationsWarning(
  schemaDir: string,
  migrationsDir?: string,
): Promise<string | null> {
  const schemaFile = rootSchemaFile(schemaDir);
  if (!schemaFile || (await importsClientMigrations(schemaFile))) return null;
  // Only reads: validate --check must not create the migrations directory.
  const dir = resolvedMigrationsDir(schemaDir, migrationsDir);
  const committed = (await new MigrationStorage(dir).listSnapshots()).filter((entry) =>
    isCommittedSnapshotFileName(entry.fileName),
  );
  if (committed.length === 0) return null;
  const shown = (path: string) => relative(schemaDir, path) || ".";
  return (
    `${basename(schemaFile)} doesn't import its migrations, so jazz-tools did not write ` +
    `${shown(clientMigrationsPath(schemaFile))} and the app ships none of the schema versions in ` +
    `${shown(dir)}. ` +
    'Add `import { migrations } from "./migrations";` to schema.ts, pass `{ migrations }` to ' +
    '`defineApp` (instead of `{ migrations: "none" }`), and run `jazz-tools validate` again.'
  );
}

/**
 * Write the app's migrations module from what this project holds locally:
 * schema.ts, its snapshots and its migration files.
 */
async function writeLocalClientMigrations(schemaDir: string, migrationsDir: string): Promise<void> {
  const compiled = await loadCompiledSchema(schemaDir);
  if (!(await importsClientMigrations(compiled.schemaFile))) return;
  const { schemas, edges } = await localClientMigrations(compiled.wasmSchema, migrationsDir);
  await writeClientMigrations(compiled.schemaFile, schemas, edges);
}

/**
 * Check the project's migrations up to `schema` as a deploy of them to a
 * new authority would, so validate refuses what deploy would refuse.
 */
async function checkLocalMigrations(schema: WasmSchema, migrationsDir: string): Promise<void> {
  const { current, schemas, edges } = await localClientMigrations(schema, migrationsDir);
  if (schemas.size < 2) return;
  if (typeof process === "undefined" || !process.versions?.node) return;
  const { checkClientMigrations } = await import("jazz-napi");
  try {
    checkClientMigrations(clientMigrationsJson(schemas, edges), current);
  } catch (error) {
    throw new Error(
      `The migrations in ${basename(migrationsDir)} would not deploy: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/** The snapshots and migration files in `migrationsDir`, leading to `schema`. */
async function localClientMigrations(
  schema: WasmSchema,
  migrationsDir: string,
): Promise<{ current: string; schemas: Map<string, WasmSchema>; edges: MigrationEdge[] }> {
  const current = {
    hash: await computeSchemaHash(schema),
    schema,
  };
  const schemas = new Map<string, WasmSchema>(
    (await new MigrationStorage(migrationsDir).listSnapshots()).map(({ hash, schema }) => [
      hash,
      schema,
    ]),
  );
  schemas.set(current.hash, current.schema);
  const edges: MigrationEdge[] = [];
  // A migration joins two known versions: with one, none can, so none is loaded.
  if (schemas.size > 1 && (await pathExists(migrationsDir))) {
    await assertNoSymlinkComponents(migrationsDir);
    for (const fileName of (await readdir(migrationsDir)).filter(isMigrationFile).sort()) {
      const migration = await loadDefinedMigration(join(migrationsDir, fileName));
      const known = (hash: string | undefined) =>
        hash === undefined
          ? undefined
          : resolveKnownSchemaHash(hash, fileName, [...schemas.keys()]);
      let fromHash, toHash;
      try {
        fromHash = known(migration.fromHash);
        toHash = known(migration.toHash);
      } catch {
        // A migration from a version this project has no snapshot of is
        // left out: run `migrations create` to snapshot it, or deploy.
        continue;
      }
      if (fromHash && toHash) edges.push({ fromHash, toHash, migration });
    }
  }
  return { current: current.hash, schemas, edges };
}

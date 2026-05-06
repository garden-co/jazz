import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { homedir, hostname } from "node:os";
import { dirname, isAbsolute, join } from "node:path";

type JsonObject = Record<string, unknown>;

type SyncJob = {
  jobId: string;
  kind: string;
  status: string;
  payloadJson?: unknown;
  attempt?: number;
};

type CommandResult = {
  exitCode: number;
  stdout: string;
  stderr: string;
};

export type CommandRunner = (
  command: string,
  args: string[],
  options?: { cwd?: string },
) => Promise<CommandResult>;

export interface RemoteAutonomyWorkerOptions {
  gatewayUrl?: string;
  workerId?: string;
  pollIntervalMs?: number;
  leaseMs?: number;
  onceLimit?: number;
  localSpacesRoot?: string;
  objectStorageMode?: "cache-only" | "oci-cli";
  fetchImpl?: typeof fetch;
  commandRunner?: CommandRunner;
}

export type RemoteAutonomyWorkerPassResult = {
  workerId: string;
  processed: number;
  completed: number;
  failed: number;
  skipped: number;
  results: Array<{
    jobId: string;
    kind: string;
    status: "completed" | "failed" | "skipped";
    error?: string;
  }>;
};

const DEFAULT_GATEWAY_URL = "http://127.0.0.1:7474";
const DEFAULT_LOCAL_SPACES_ROOT = join(homedir(), ".designer", "spaces");
const WORKER_SOURCE = "remote-autonomy-gateway-worker";
const SUPPORTED_KINDS = new Set([
  "rsync-mirror",
  "space-rsync-mirror",
  "space-file-object-upload",
  "space-file-materialize",
]);
const JOB_PRIORITY: Record<string, number> = {
  "rsync-mirror": 0,
  "space-rsync-mirror": 0,
  "space-file-object-upload": 1,
  "space-file-materialize": 2,
};

export function startRemoteAutonomyWorker(options: RemoteAutonomyWorkerOptions = {}) {
  const pollIntervalMs = Math.max(250, options.pollIntervalMs ?? 2_000);
  let stopped = false;
  let running = false;
  let timer: NodeJS.Timeout | null = null;

  const tick = async () => {
    if (stopped || running) {
      return;
    }
    running = true;
    try {
      await runRemoteAutonomyWorkerOnce(options);
    } catch (error) {
      console.error(
        JSON.stringify({
          ok: false,
          source: WORKER_SOURCE,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    } finally {
      running = false;
      if (!stopped) {
        timer = setTimeout(tick, pollIntervalMs);
      }
    }
  };

  timer = setTimeout(tick, 0);
  return {
    stop: () => {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
    },
  };
}

export async function runRemoteAutonomyWorkerOnce(
  options: RemoteAutonomyWorkerOptions = {},
): Promise<RemoteAutonomyWorkerPassResult> {
  const resolved = resolveWorkerOptions(options);
  const queued = await gatewayGet<{ jobs: SyncJob[] }>(
    resolved,
    `/v1/sync/jobs?status=queued&limit=${resolved.onceLimit}`,
  );
  const jobs = queued.jobs.filter((job) => SUPPORTED_KINDS.has(job.kind)).sort(compareSyncJobs);
  const result: RemoteAutonomyWorkerPassResult = {
    workerId: resolved.workerId,
    processed: 0,
    completed: 0,
    failed: 0,
    skipped: queued.jobs.length - jobs.length,
    results: [],
  };

  for (const job of jobs) {
    result.processed += 1;
    try {
      const claimed = await claimJob(resolved, job);
      await updateJob(resolved, claimed.job, "running", {
        source: WORKER_SOURCE,
        workerId: resolved.workerId,
      });
      const receipt = await executeJob(resolved, claimed.job);
      await postReceipt(resolved, claimed.job.jobId, receipt);
      result.completed += 1;
      result.results.push({
        jobId: claimed.job.jobId,
        kind: claimed.job.kind,
        status: "completed",
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      result.failed += 1;
      result.results.push({
        jobId: job.jobId,
        kind: job.kind,
        status: "failed",
        error: message,
      });
      await updateJob(resolved, job, "failed", {
        source: WORKER_SOURCE,
        workerId: resolved.workerId,
        error: message,
      }).catch(() => undefined);
    }
  }

  return result;
}

type ResolvedWorkerOptions = Required<
  Pick<
    RemoteAutonomyWorkerOptions,
    | "gatewayUrl"
    | "workerId"
    | "leaseMs"
    | "onceLimit"
    | "localSpacesRoot"
    | "objectStorageMode"
    | "fetchImpl"
    | "commandRunner"
  >
>;

function resolveWorkerOptions(options: RemoteAutonomyWorkerOptions): ResolvedWorkerOptions {
  return {
    gatewayUrl: stripTrailingSlash(
      options.gatewayUrl ?? process.env.REMOTE_AUTONOMY_GATEWAY_URL ?? DEFAULT_GATEWAY_URL,
    ),
    workerId:
      options.workerId ??
      process.env.REMOTE_AUTONOMY_WORKER_ID ??
      `${hostname()}:remote-autonomy-worker`,
    leaseMs: options.leaseMs ?? 5 * 60 * 1000,
    onceLimit: options.onceLimit ?? 20,
    localSpacesRoot:
      options.localSpacesRoot ??
      process.env.REMOTE_AUTONOMY_LOCAL_SPACES_ROOT ??
      DEFAULT_LOCAL_SPACES_ROOT,
    objectStorageMode:
      options.objectStorageMode ??
      (process.env.REMOTE_AUTONOMY_OBJECT_STORAGE_MODE === "oci-cli" ? "oci-cli" : "cache-only"),
    fetchImpl: options.fetchImpl ?? fetch,
    commandRunner: options.commandRunner ?? runCommand,
  };
}

function compareSyncJobs(left: SyncJob, right: SyncJob): number {
  const priorityDelta = (JOB_PRIORITY[left.kind] ?? 99) - (JOB_PRIORITY[right.kind] ?? 99);
  if (priorityDelta !== 0) {
    return priorityDelta;
  }
  return left.jobId.localeCompare(right.jobId);
}

async function executeJob(options: ResolvedWorkerOptions, job: SyncJob): Promise<JsonObject> {
  const payload = objectPayload(job);
  switch (job.kind) {
    case "rsync-mirror":
    case "space-rsync-mirror":
      return await executeRsyncJob(options, payload);
    case "space-file-object-upload":
      return await executeObjectUploadJob(options, payload);
    case "space-file-materialize":
      return await executeMaterializeJob(options, payload);
    default:
      throw new Error(`unsupported sync job kind ${job.kind}`);
  }
}

async function executeRsyncJob(
  options: ResolvedWorkerOptions,
  payload: JsonObject,
): Promise<JsonObject> {
  const sourcePath = requiredPath(payload, "sourcePath");
  const targetPath = requiredPath(payload, "targetPath");
  await mkdir(sourcePath, { recursive: true });
  await mkdir(targetPath, { recursive: true });
  const sourceArg = sourcePath.endsWith("/") ? sourcePath : `${sourcePath}/`;
  const targetArg = targetPath.endsWith("/") ? targetPath : `${targetPath}/`;
  const command = await options.commandRunner("rsync", ["-a", "--delete", sourceArg, targetArg]);
  if (command.exitCode !== 0) {
    throw new Error(`rsync failed: ${command.stderr || command.stdout || command.exitCode}`);
  }
  const summary = await directorySummary(targetPath);
  return {
    status: "completed",
    transport: "rsync",
    sourcePath,
    targetPath,
    checksum: summary.digest,
    bytes: summary.bytes,
    fileCount: summary.fileCount,
    stdout: command.stdout,
    stderr: command.stderr,
  };
}

async function executeObjectUploadJob(
  options: ResolvedWorkerOptions,
  payload: JsonObject,
): Promise<JsonObject> {
  const file = objectField(payload, "file");
  const objectStorage = objectField(payload, "objectStorage");
  const objectKey = requiredString(objectStorage, "key");
  const targetUri = requiredString(payload, "targetUri");
  const expectedHash = requiredString(file, "contentHash");
  const sourcePath = await firstReadablePath([
    optionalPath(payload, "sourcePath"),
    optionalString(file, "remotePath"),
    optionalString(file, "localPath"),
  ]);
  const bytes = await readFile(sourcePath);
  const checksum = sha256(bytes);
  if (checksum !== expectedHash) {
    throw new Error(`object upload checksum mismatch: got ${checksum}, expected ${expectedHash}`);
  }
  const cachePath = objectCachePath(options, objectKey);
  await writeFileWithParents(cachePath, bytes);
  if (options.objectStorageMode === "oci-cli") {
    await putOciObject(options, objectStorage, cachePath);
  }
  return {
    status: "completed",
    transport: options.objectStorageMode === "oci-cli" ? "oci-cli" : "object-cache",
    sourcePath,
    targetPath: targetUri,
    cachePath,
    checksum,
    bytes: bytes.byteLength,
  };
}

async function executeMaterializeJob(
  options: ResolvedWorkerOptions,
  payload: JsonObject,
): Promise<JsonObject> {
  const file = objectField(payload, "file");
  const objectStorage = objectField(payload, "objectStorage");
  const objectKey = requiredString(objectStorage, "key");
  const expectedHash = requiredString(file, "contentHash");
  const targetPath = requiredPath(payload, "targetPath");
  const cachePath = objectCachePath(options, objectKey);
  let bytes = await readFileOrNull(cachePath);
  if (!bytes && options.objectStorageMode === "oci-cli") {
    await mkdir(dirname(cachePath), { recursive: true });
    await getOciObject(options, objectStorage, cachePath);
    bytes = await readFileOrNull(cachePath);
  }
  if (!bytes) {
    throw new Error(`cached object not found for ${objectKey}`);
  }
  const checksum = sha256(bytes);
  if (checksum !== expectedHash) {
    throw new Error(`materialize checksum mismatch: got ${checksum}, expected ${expectedHash}`);
  }
  await writeFileWithParents(targetPath, bytes);
  return {
    status: "completed",
    transport: options.objectStorageMode === "oci-cli" ? "oci-cli" : "object-cache",
    sourcePath: requiredString(payload, "sourceUri"),
    targetPath,
    cachePath,
    checksum,
    bytes: bytes.byteLength,
  };
}

async function claimJob(options: ResolvedWorkerOptions, job: SyncJob): Promise<{ job: SyncJob }> {
  return await gatewayPost(options, `/v1/sync/jobs/${encodeURIComponent(job.jobId)}/claim`, {
    claimedBy: options.workerId,
    leaseExpiresAt: new Date(Date.now() + options.leaseMs).toISOString(),
    note: WORKER_SOURCE,
  });
}

async function updateJob(
  options: ResolvedWorkerOptions,
  job: SyncJob,
  status: string,
  resultJson: JsonObject,
): Promise<{ job: SyncJob }> {
  return await gatewayPost(options, `/v1/sync/jobs/${encodeURIComponent(job.jobId)}/status`, {
    status,
    claimedBy: status === "running" ? options.workerId : undefined,
    resultJson,
    note: `${WORKER_SOURCE} ${status}`,
  });
}

async function postReceipt(
  options: ResolvedWorkerOptions,
  jobId: string,
  receipt: JsonObject,
): Promise<void> {
  await gatewayPost(options, "/v1/sync/receipts", {
    jobId,
    status: "completed",
    transport: requiredString(receipt, "transport"),
    sourcePath: optionalString(receipt, "sourcePath"),
    targetPath: optionalString(receipt, "targetPath"),
    checksum: optionalString(receipt, "checksum"),
    bytes: optionalNumber(receipt, "bytes"),
    hostId: options.workerId,
    payloadJson: {
      ...receipt,
      workerId: options.workerId,
    },
  });
}

async function gatewayGet<T>(options: ResolvedWorkerOptions, path: string): Promise<T> {
  const response = await options.fetchImpl(`${options.gatewayUrl}${path}`);
  return await parseResponse<T>(response, path);
}

async function gatewayPost<T>(
  options: ResolvedWorkerOptions,
  path: string,
  body: JsonObject,
): Promise<T> {
  const response = await options.fetchImpl(`${options.gatewayUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return await parseResponse<T>(response, path);
}

async function parseResponse<T>(response: Response, path: string): Promise<T> {
  const text = await response.text();
  const json = text ? JSON.parse(text) : {};
  if (!response.ok) {
    const message =
      json && typeof json === "object" && "error" in json
        ? String((json as { error: unknown }).error)
        : text;
    throw new Error(`${path} failed: ${message}`);
  }
  return json as T;
}

async function runCommand(
  command: string,
  args: string[],
  options: { cwd?: string } = {},
): Promise<CommandResult> {
  return await new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      resolve({ exitCode: 127, stdout, stderr: error.message });
    });
    child.on("close", (code) => {
      resolve({ exitCode: code ?? 1, stdout, stderr });
    });
  });
}

async function putOciObject(
  options: ResolvedWorkerOptions,
  objectStorage: JsonObject,
  filePath: string,
): Promise<void> {
  const args = [
    "os",
    "object",
    "put",
    "--bucket-name",
    requiredString(objectStorage, "bucket"),
    "--name",
    requiredString(objectStorage, "key"),
    "--file",
    filePath,
    "--force",
    "--region",
    requiredString(objectStorage, "region"),
  ];
  const result = await options.commandRunner("oci", args);
  if (result.exitCode !== 0) {
    throw new Error(`oci object put failed: ${result.stderr || result.stdout || result.exitCode}`);
  }
}

async function getOciObject(
  options: ResolvedWorkerOptions,
  objectStorage: JsonObject,
  filePath: string,
): Promise<void> {
  const args = [
    "os",
    "object",
    "get",
    "--bucket-name",
    requiredString(objectStorage, "bucket"),
    "--name",
    requiredString(objectStorage, "key"),
    "--file",
    filePath,
    "--region",
    requiredString(objectStorage, "region"),
  ];
  const result = await options.commandRunner("oci", args);
  if (result.exitCode !== 0) {
    throw new Error(`oci object get failed: ${result.stderr || result.stdout || result.exitCode}`);
  }
}

async function directorySummary(root: string): Promise<{
  bytes: number;
  fileCount: number;
  digest: string;
}> {
  const hash = createHash("sha256");
  let bytes = 0;
  let fileCount = 0;
  const walk = async (dir: string, relativePrefix = "") => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
      const absolute = join(dir, entry.name);
      const relative = relativePrefix ? `${relativePrefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        await walk(absolute, relative);
      } else if (entry.isFile()) {
        const content = await readFile(absolute);
        hash.update(relative);
        hash.update("\0");
        hash.update(content);
        bytes += content.byteLength;
        fileCount += 1;
      }
    }
  };
  await walk(root);
  return {
    bytes,
    fileCount,
    digest: `sha256:${hash.digest("hex")}`,
  };
}

async function firstReadablePath(paths: Array<string | undefined>): Promise<string> {
  for (const path of paths) {
    if (!path) {
      continue;
    }
    safeAbsolutePath(path, "sourcePath");
    try {
      const fileStat = await stat(path);
      if (fileStat.isFile()) {
        return path;
      }
    } catch {
      continue;
    }
  }
  throw new Error("no readable source file found for object upload");
}

async function writeFileWithParents(filePath: string, bytes: Buffer): Promise<void> {
  safeAbsolutePath(filePath, "targetPath");
  await mkdir(dirname(filePath), { recursive: true });
  await writeFile(filePath, bytes);
}

async function readFileOrNull(filePath: string): Promise<Buffer | null> {
  try {
    return await readFile(filePath);
  } catch {
    return null;
  }
}

function objectCachePath(options: ResolvedWorkerOptions, key: string): string {
  const segments = key.split("/").filter(Boolean);
  if (segments.length === 0 || segments.some((segment) => segment === "." || segment === "..")) {
    throw new Error(`invalid object storage key ${key}`);
  }
  return join(options.localSpacesRoot, ".object-cache", ...segments);
}

function objectPayload(job: SyncJob): JsonObject {
  const payload = jsonObject(job.payloadJson);
  if (!payload) {
    throw new Error(`job ${job.jobId} has no object payload`);
  }
  return payload;
}

function objectField(source: JsonObject, key: string): JsonObject {
  const value = jsonObject(source[key]);
  if (!value) {
    throw new Error(`missing object field ${key}`);
  }
  return value;
}

function requiredString(source: JsonObject, key: string): string {
  const value = source[key];
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`missing string field ${key}`);
  }
  return value;
}

function optionalString(source: JsonObject, key: string): string | undefined {
  const value = source[key];
  return typeof value === "string" && value.trim() ? value : undefined;
}

function optionalNumber(source: JsonObject, key: string): number | undefined {
  const value = source[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function requiredPath(source: JsonObject, key: string): string {
  return safeAbsolutePath(requiredString(source, key), key);
}

function optionalPath(source: JsonObject, key: string): string | undefined {
  const value = optionalString(source, key);
  return value ? safeAbsolutePath(value, key) : undefined;
}

function safeAbsolutePath(path: string, field: string): string {
  if (!isAbsolute(path) || path.includes("\0")) {
    throw new Error(`${field} must be an absolute path`);
  }
  return path;
}

function jsonObject(value: unknown): JsonObject | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : null;
}

function sha256(bytes: Buffer): string {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

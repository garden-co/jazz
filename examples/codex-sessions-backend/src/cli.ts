import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { createReadStream, readFileSync, rmSync, writeFileSync } from "node:fs";
import { mkdir, readFile, readdir, stat, watch as watchPath } from "node:fs/promises";
import { createConnection, createServer, type Socket } from "node:net";
import { homedir, hostname } from "node:os";
import { basename, dirname, extname, join, resolve } from "node:path";
import type {
  CodexSession,
  JAgentArtifact,
  JAgentAttempt,
  JAgentDefinition,
  JAgentRun,
  JAgentSessionBinding,
  JAgentStep,
  JAgentWait,
  JsonValue,
} from "../schema/app.js";
import {
  type BindJAgentSessionInput,
  createCodexSessionStore,
  type CodexCompletionEvent,
  type CodexSessionStoreConfig,
  type JAgentRunSummary,
  type RecordCodexStreamEventInput,
  type RecordCodexTerminalPresenceInput,
  type RecordJAgentArtifactInput,
  type RecordJAgentAttemptCompletedInput,
  type RecordJAgentAttemptStartedInput,
  type RecordJAgentRunCompletedInput,
  type RecordJAgentRunStartedInput,
  type RecordJAgentStepCompletedInput,
  type RecordJAgentStepStartedInput,
  type RecordJAgentWaitResolvedInput,
  type RecordJAgentWaitStartedInput,
  type UpsertJAgentDefinitionInput,
} from "./store.js";
import {
  buildSessionProjectionFromRollout,
  type SyncedProjectionEvent,
  syncCodexRollouts,
  syncCodexSessionRollout,
  syncNewestCodexRollouts,
  syncRecentSessionsForProjectRoot,
  syncSessionsByPrefix,
  watchCodexRollouts,
} from "./projector.js";
import { collectRecentCompletionEvents, trackEmittedId } from "./completion-watcher.js";

interface SessionLookupRow {
  id: string;
  cwd: string;
  projectRoot: string;
  updatedAt: number;
  updatedLabel: string;
  branch: string;
  model: string;
  preview: string;
}

interface SessionCandidateRow extends SessionLookupRow {
  matchRank: number | null;
}

interface SessionReferenceRow extends SessionLookupRow {
  repoRoot: string | null;
  status: string;
  startedAt: string;
  completedAt: string | null;
}

interface RolloutEventRow {
  id: string;
  sourceId: string;
  absolutePath: string;
  sessionId: string | null;
  lineNumber: number;
  byteOffset: number;
  byteLength: number;
  timestamp: string | null;
  recordType: string;
  eventType: string | null;
  turnId: string | null;
  payloadJson: unknown | null;
  rawJson: string;
  createdAt: string;
}

interface RolloutEventListInput {
  sessionId?: string;
  absolutePath?: string;
  afterLineNumber?: number;
  afterByteOffset?: number;
  limit?: number;
}

interface ReplicateRolloutEventsInput extends RolloutEventListInput {
  follow?: boolean;
  idleTimeoutMs?: number;
  maxBatches?: number;
  pollIntervalMs: number;
  sourceHost?: string;
  yieldBetweenEvents?: boolean;
}

interface ReplicateRolloutEventsResult {
  absolutePath: string | null;
  sessionId: string | null;
  recorded: number;
  lastLineNumber: number | null;
  lastByteOffset: number | null;
  followed: boolean;
  hasMore?: boolean;
}

interface SessionServiceRequest {
  id?: string;
  method: string;
  projectRoot?: string;
  query?: string;
  prefix?: string;
  sessionId?: string;
  turnId?: string;
  absolutePath?: string;
  afterLineNumber?: number;
  afterByteOffset?: number;
  afterSequence?: number;
  follow?: boolean;
  idleTimeoutMs?: number;
  pollIntervalMs?: number;
  sourceHost?: string;
  runId?: string;
  completedAfter?: string;
  limit?: number;
  latest?: boolean;
  includePayload?: boolean;
  payload?: unknown;
}

interface SessionServiceResponse {
  id?: string;
  ok: boolean;
  result?: unknown;
  event?: unknown;
  error?: string;
}

interface SessionServiceInstanceLock {
  lockPath: string;
  metadata: SessionServiceLockMetadata;
}

interface SessionServiceLockMetadata {
  pid: number;
  socketPath: string;
  startedAt: string;
}

interface SessionServiceRuntimeInfo {
  socketPath: string;
  watchRollouts: boolean;
  watchStreamRollouts: boolean;
  streamDataPath: string;
}

type CodexSessionStoreHandle = ReturnType<typeof createCodexSessionStore>;
type CodexStreamEventRow = Awaited<ReturnType<CodexSessionStoreHandle["listCodexStreamEvents"]>>[number];

interface SessionServiceRuntimeDeps {
  store: CodexSessionStoreHandle;
  sessionSyncScheduler: SessionSyncScheduler;
  catalogPrimer: CatalogPrimer;
  recentRolloutSyncScheduler: RecentRolloutSyncScheduler;
}

interface SessionSyncScheduler {
  syncSession(sessionId: string): Promise<{ scanned: number; synced: number; found: boolean }>;
}

interface CatalogPrimer {
  ensurePrimed(): Promise<void>;
}

interface RecentRolloutSyncRequest {
  limit: number;
  maxWaitMs?: number;
}

interface RecentRolloutSyncScheduler {
  ensureFresh(request: RecentRolloutSyncRequest): Promise<void>;
}

interface JAgentDefinitionRow {
  definitionId: string;
  name: string;
  version: string;
  sourceKind: string;
  entrypoint: string;
  metadataJson: unknown | null;
  createdAt: string;
  updatedAt: string;
}

interface JAgentRunRow {
  runId: string;
  definitionId: string;
  status: string;
  projectRoot: string;
  repoRoot: string | null;
  cwd: string | null;
  triggerSource: string | null;
  parentSessionId: string | null;
  parentTurnId: string | null;
  initiatorSessionId: string | null;
  requestedRole: string | null;
  requestedModel: string | null;
  requestedReasoningEffort: string | null;
  forkTurns: number | null;
  currentStepKey: string | null;
  inputJson: unknown | null;
  outputJson: unknown | null;
  errorText: string | null;
  startedAt: string;
  updatedAt: string;
  completedAt: string | null;
}

interface JAgentStepRow {
  stepId: string;
  runId: string;
  sequence: number;
  stepKey: string;
  stepKind: string;
  status: string;
  inputJson: unknown | null;
  outputJson: unknown | null;
  errorText: string | null;
  startedAt: string;
  updatedAt: string;
  completedAt: string | null;
}

interface JAgentAttemptRow {
  attemptId: string;
  runId: string;
  stepId: string;
  attempt: number;
  status: string;
  codexSessionId: string | null;
  codexTurnId: string | null;
  forkTurns: number | null;
  modelName: string | null;
  reasoningEffort: string | null;
  startedAt: string;
  completedAt: string | null;
  errorText: string | null;
}

interface JAgentWaitRow {
  waitId: string;
  runId: string;
  stepId: string;
  waitKind: string;
  targetSessionId: string | null;
  targetTurnId: string | null;
  resumeConditionJson: unknown | null;
  status: string;
  startedAt: string;
  resumedAt: string | null;
}

interface JAgentSessionBindingRow {
  bindingId: string;
  runId: string;
  codexSessionId: string;
  bindingRole: string;
  parentSessionId: string | null;
  createdAt: string;
}

interface JAgentArtifactRow {
  artifactId: string;
  runId: string;
  stepId: string | null;
  kind: string;
  path: string;
  textPreview: string | null;
  metadataJson: unknown | null;
  createdAt: string;
}

interface JAgentRunSummaryRow {
  definition: JAgentDefinitionRow;
  run: JAgentRunRow;
  steps: JAgentStepRow[];
  attempts: JAgentAttemptRow[];
  waits: JAgentWaitRow[];
  sessionBindings: JAgentSessionBindingRow[];
  artifacts: JAgentArtifactRow[];
  boundSessions: SessionReferenceRow[];
}

const SESSION_SERVICE_PRIME_BUDGET_MS = 2_000;
const SESSION_SERVICE_STARTUP_WAIT_MS = 2_000;
const SESSION_SERVICE_STALE_LOCK_GRACE_MS = 30_000;
const SESSION_SERVICE_REQUEST_TIMEOUT_MS = 2_500;
const ACTIVE_SESSION_RECENT_SYNC_LIMIT = 32;
const ACTIVE_SESSION_RECENT_SYNC_BUDGET_MS = 400;

function logBackgroundError(label: string, error: unknown): void {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  console.error(`${label}: ${message}`);
}

async function waitForPromiseWithinBudget<T>(
  promise: Promise<T>,
  maxWaitMs: number | undefined,
): Promise<void> {
  if (maxWaitMs === undefined || maxWaitMs <= 0) {
    await promise;
    return;
  }

  await Promise.race([
    promise.then(() => undefined),
    new Promise<void>((resolve) => {
      setTimeout(resolve, maxWaitMs);
    }),
  ]);
}

function activeSessionFreshSyncLimit(limit: number): number {
  return Math.max(
    8,
    Math.min(Math.max(1, Math.trunc(limit)) * 2, ACTIVE_SESSION_RECENT_SYNC_LIMIT),
  );
}

function warmActiveSessionPresenceInBackground(
  store: ReturnType<typeof createCodexSessionStore>,
  projectRoot: string | undefined,
): void {
  void store
    .backfillSessionPresence(projectRoot ? { projectRoot } : undefined)
    .catch((error: unknown) => {
      logBackgroundError("active session presence backfill failed", error);
    });
}

function expandHomePath(path: string): string {
  if (path === "~") {
    return homedir();
  }
  if (path.startsWith("~/")) {
    return resolve(homedir(), path.slice(2));
  }
  return resolve(path);
}

function defaultFlowSupportDirectory(): string {
  if (process.platform === "darwin") {
    return resolve(homedir(), "Library", "Application Support", "Flow");
  }
  return resolve(homedir(), ".local", "share", "flow");
}

function defaultFlowDataDirectory(): string {
  if (process.platform === "darwin") {
    return resolve(homedir(), "Library", "Caches", "Flow");
  }
  return defaultFlowSupportDirectory();
}

function defaultJazzDataPath(): string {
  return resolve(defaultFlowDataDirectory(), "codex-sessions.db");
}

function defaultStreamDataPath(dataPath: string): string {
  const extension = extname(dataPath);
  if (!extension) {
    return `${dataPath}.stream`;
  }
  return `${dataPath.slice(0, -extension.length)}.stream${extension}`;
}

function defaultJazzSocketPath(): string {
  return resolve(defaultFlowSupportDirectory(), "codex-sessions.sock");
}

function legacyDirectoryFallbackPath(dataPath: string): string {
  if (dataPath.endsWith(".db")) {
    return `${dataPath.slice(0, -3)}.sqlite`;
  }
  return `${dataPath}.sqlite`;
}

async function resolvePersistentDataPath(dataPath: string): Promise<string> {
  const normalizedPath = expandHomePath(dataPath);
  const currentStat = await stat(normalizedPath).catch((error: unknown) => {
    const code =
      typeof error === "object" && error !== null && "code" in error
        ? String((error as { code?: unknown }).code ?? "")
        : "";
    if (code === "ENOENT") {
      return null;
    }
    throw error;
  });

  if (currentStat?.isDirectory()) {
    const fallbackPath = legacyDirectoryFallbackPath(normalizedPath);
    const fallbackStat = await stat(fallbackPath).catch((error: unknown) => {
      const code =
        typeof error === "object" && error !== null && "code" in error
          ? String((error as { code?: unknown }).code ?? "")
          : "";
      if (code === "ENOENT") {
        return null;
      }
      throw error;
    });
    if (fallbackStat?.isDirectory()) {
      throw new Error(
        `Jazz data path ${normalizedPath} is a directory, and fallback path ${fallbackPath} is also a directory`,
      );
    }
    await mkdir(dirname(fallbackPath), { recursive: true });
    console.error(
      `warning: Jazz2 data path ${normalizedPath} is a directory; using ${fallbackPath} instead`,
    );
    return fallbackPath;
  }

  await mkdir(dirname(normalizedPath), { recursive: true });
  return normalizedPath;
}

function backendSchemaHash(): string {
  const candidates = [
    new URL("../schema/current.js", import.meta.url),
    new URL("../schema/current.ts", import.meta.url),
    new URL("../schema/current.sql", import.meta.url),
    new URL("../schema/app.js", import.meta.url),
    new URL("../schema/app.ts", import.meta.url),
  ];

  const errors: string[] = [];
  for (const candidate of candidates) {
    try {
      const data = readFileSync(candidate);
      return createHash("sha256").update(data).digest("hex");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`${candidate.pathname}: ${message}`);
    }
  }

  throw new Error(`Could not compute codex-sessions schema hash: ${errors.join("; ")}`);
}

function printUsage(): void {
  console.log("Usage: codex-sessions-backend <serve|sync|sync-session|schema-hash|list-sessions|search-sessions|...>");
}

function readFlag(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  if (index === -1) {
    return undefined;
  }
  return process.argv[index + 1];
}

function readBooleanFlag(flag: string, fallback: boolean): boolean {
  const raw = readFlag(flag);
  if (!raw) {
    return fallback;
  }

  const normalized = raw.trim().toLowerCase();
  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on") {
    return true;
  }
  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "off") {
    return false;
  }
  throw new Error(`${flag} must be a boolean value`);
}

function readOptionalNumberFlag(flag: string): number | undefined {
  const raw = readFlag(flag);
  if (raw === undefined || raw.trim() === "") {
    return undefined;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${flag} must be a finite number`);
  }
  return parsed;
}

function readFlagOrEnv(flag: string, ...envNames: string[]): string | undefined {
  const flagValue = readFlag(flag);
  if (flagValue !== undefined) {
    return flagValue;
  }
  for (const envName of envNames) {
    const value = process.env[envName];
    if (value !== undefined && value.trim() !== "") {
      return value;
    }
  }
  return undefined;
}

function readStoreTier(): CodexSessionStoreConfig["tier"] | undefined {
  const raw = readFlagOrEnv("--tier", "FLOW_CODEX_JAZZ_TIER", "J_SESSIONS_JAZZ_TIER");
  if (raw === undefined) {
    return undefined;
  }
  if (raw === "edge" || raw === "global" || raw === "local") {
    return raw;
  }
  throw new Error("--tier must be one of edge, global, or local");
}

function readStoreConfig(dataPath: string): CodexSessionStoreConfig {
  return {
    dataPath,
    appId: readFlagOrEnv("--app-id", "FLOW_CODEX_JAZZ_APP_ID", "J_SESSIONS_JAZZ_APP_ID"),
    env: readFlagOrEnv("--env", "FLOW_CODEX_JAZZ_ENV", "J_SESSIONS_JAZZ_ENV"),
    userBranch: readFlagOrEnv("--user-branch", "FLOW_CODEX_JAZZ_USER_BRANCH", "J_SESSIONS_JAZZ_USER_BRANCH"),
    serverUrl: readFlagOrEnv("--server-url", "FLOW_CODEX_JAZZ_SERVER_URL", "J_SESSIONS_JAZZ_SERVER_URL"),
    serverPathPrefix: readFlagOrEnv(
      "--server-path-prefix",
      "FLOW_CODEX_JAZZ_SERVER_PATH_PREFIX",
      "J_SESSIONS_JAZZ_SERVER_PATH_PREFIX",
    ),
    backendSecret: readFlagOrEnv("--backend-secret", "FLOW_CODEX_JAZZ_BACKEND_SECRET", "J_SESSIONS_JAZZ_BACKEND_SECRET"),
    adminSecret: readFlagOrEnv("--admin-secret", "FLOW_CODEX_JAZZ_ADMIN_SECRET", "J_SESSIONS_JAZZ_ADMIN_SECRET"),
    tier: readStoreTier(),
  };
}

function localStreamStoreConfig(
  config: CodexSessionStoreConfig,
  streamDataPath: string,
): CodexSessionStoreConfig {
  return {
    dataPath: streamDataPath,
    appId: config.appId,
    env: config.env,
    userBranch: config.userBranch,
    tier: "local",
  };
}

function readJsonInput<T>(command: string): T {
  const inlineJson = readFlag("--input-json");
  const inputFile = readFlag("--input-file");

  if (inlineJson && inputFile) {
    throw new Error(`${command} accepts only one of --input-json or --input-file`);
  }

  const text = inputFile
    ? readFileSync(expandHomePath(inputFile), "utf8")
    : inlineJson
      ? inlineJson
      : !process.stdin.isTTY
        ? readFileSync(0, "utf8")
        : null;

  if (!text) {
    throw new Error(`${command} requires --input-json, --input-file, or stdin JSON`);
  }

  try {
    return JSON.parse(text) as T;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${command} received invalid JSON: ${message}`);
  }
}

function asIsoString(value: Date): string {
  return value.toISOString();
}

function asNullableIsoString(value: Date | undefined): string | null {
  return value ? value.toISOString() : null;
}

function nullish<T>(value: T | undefined): T | null {
  return value ?? null;
}

function normalizeRequiredPath(path: string): string {
  return expandHomePath(path);
}

function normalizeOptionalPath(path: string | undefined): string | undefined {
  return path ? expandHomePath(path) : undefined;
}

function normalizeDefinitionInput(input: UpsertJAgentDefinitionInput): UpsertJAgentDefinitionInput {
  return {
    ...input,
    entrypoint: input.entrypoint.startsWith("~") ? expandHomePath(input.entrypoint) : input.entrypoint,
  };
}

function normalizeRunStartedInput(input: RecordJAgentRunStartedInput): RecordJAgentRunStartedInput {
  return {
    ...input,
    projectRoot: normalizeRequiredPath(input.projectRoot),
    repoRoot: normalizeOptionalPath(input.repoRoot),
    cwd: normalizeOptionalPath(input.cwd),
  };
}

function normalizeArtifactInput(input: RecordJAgentArtifactInput): RecordJAgentArtifactInput {
  return {
    ...input,
    path: normalizeRequiredPath(input.path),
  };
}

function normalizeRecordCodexTerminalPresenceInput(
  input: RecordCodexTerminalPresenceInput,
): RecordCodexTerminalPresenceInput {
  return {
    ...input,
    cwd: normalizeOptionalPath(input.cwd),
    projectRoot: normalizeOptionalPath(input.projectRoot),
    repoRoot: normalizeOptionalPath(input.repoRoot),
    activityPath: normalizeOptionalPath(input.activityPath),
  };
}

function normalizeRecordCodexStreamEventInput(input: Record<string, unknown>): RecordCodexStreamEventInput {
  const sessionId = stringField(input, "sessionId", "session_id");
  const eventKind = stringField(input, "eventKind", "event_kind");
  const sequence = numberField(input, "sequence");
  if (!sessionId) {
    throw new Error("record-event requires sessionId");
  }
  if (!eventKind) {
    throw new Error("record-event requires eventKind");
  }
  if (sequence === undefined) {
    throw new Error("record-event requires sequence");
  }

  return {
    eventId: stringField(input, "eventId", "event_id"),
    sessionId,
    turnId: stringField(input, "turnId", "turn_id"),
    sequence,
    eventKind,
    eventType: stringField(input, "eventType", "event_type"),
    sourceId: stringField(input, "sourceId", "source_id"),
    sourceHost: stringField(input, "sourceHost", "source_host"),
    sourcePath: stringField(input, "sourcePath", "source_path"),
    textDelta: stringField(input, "textDelta", "text_delta"),
    payloadJson: jsonField(input, "payloadJson", "payload_json"),
    rawJson: jsonField(input, "rawJson", "raw_json"),
    schemaHash: stringField(input, "schemaHash", "schema_hash"),
    createdAt: timestampField(input, "createdAt", "created_at"),
    observedAt: timestampField(input, "observedAt", "observed_at"),
  };
}

function field(input: Record<string, unknown>, ...keys: string[]): unknown {
  for (const key of keys) {
    if (input[key] !== undefined) {
      return input[key];
    }
  }
  return undefined;
}

function stringField(input: Record<string, unknown>, ...keys: string[]): string | undefined {
  const value = field(input, ...keys);
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function numberField(input: Record<string, unknown>, ...keys: string[]): number | undefined {
  const value = field(input, ...keys);
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function jsonField(input: Record<string, unknown>, ...keys: string[]): JsonValue | undefined {
  const value = field(input, ...keys);
  return value === undefined ? undefined : value as JsonValue;
}

function timestampField(
  input: Record<string, unknown>,
  ...keys: string[]
): RecordCodexStreamEventInput["createdAt"] | undefined {
  const value = field(input, ...keys);
  if (
    value instanceof Date
    || typeof value === "string"
    || typeof value === "number"
  ) {
    return value;
  }
  return undefined;
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function normalizeQuery(query: string): string {
  return query
    .split(/\s+/)
    .filter(Boolean)
    .join(" ")
    .trim()
    .toLowerCase();
}

function cleanText(text: string | null | undefined): string {
  return (text ?? "").split(/\s+/).filter(Boolean).join(" ");
}

function clipText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 3))}...`;
}

function previewForSession(session: CodexSession): string {
  const source =
    session.latest_preview ??
    session.latest_assistant_partial ??
    session.latest_assistant_message ??
    session.latest_user_message ??
    session.first_user_message ??
    "(no prompt text)";
  return clipText(cleanText(source), 100);
}

function modelLabel(session: CodexSession): string {
  const modelName = session.model_name?.trim() || session.model_provider?.trim() || "";
  const reasoningEffort = session.reasoning_effort?.trim() || "";
  if (modelName && reasoningEffort) {
    return `${modelName}/${reasoningEffort}`;
  }
  if (modelName) {
    return modelName;
  }
  if (reasoningEffort) {
    return reasoningEffort;
  }
  return "-";
}

function latestMessageTimestamp(session: CodexSession): Date {
  const messageTimestamps = [session.last_assistant_at, session.last_user_at]
    .filter((value): value is Date => value instanceof Date)
    .map((value) => value.getTime())
    .filter((value) => !Number.isNaN(value));

  if (messageTimestamps.length > 0) {
    return new Date(Math.max(...messageTimestamps));
  }

  return session.latest_activity_at ?? session.updated_at;
}

function formatLocalTimestamp(value: Date): string {
  return value.toLocaleString("sv-SE", {
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function toSessionLookupRow(session: CodexSession): SessionLookupRow {
  const updatedAt = latestMessageTimestamp(session);
  return {
    id: session.session_id,
    cwd: session.cwd,
    projectRoot: session.project_root,
    updatedAt: Math.floor(updatedAt.getTime() / 1000),
    updatedLabel: formatLocalTimestamp(updatedAt),
    branch: session.git_branch?.trim() || "-",
    model: modelLabel(session),
    preview: previewForSession(session),
  };
}

function toSessionReferenceRow(session: CodexSession): SessionReferenceRow {
  return {
    ...toSessionLookupRow(session),
    repoRoot: nullish(session.repo_root),
    status: session.status,
    startedAt: asIsoString(session.created_at),
    completedAt: asNullableIsoString(session.last_completion_at),
  };
}

function sessionIdFromRolloutPath(rolloutPath: string): string | null {
  const match = rolloutPath.match(
    /([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\.jsonl$/i,
  );
  return match?.[1] ?? null;
}

function rolloutEventTypeFromParsed(parsed: Record<string, unknown>): string | null {
  const payload = typeof parsed.payload === "object" && parsed.payload !== null
    ? parsed.payload as Record<string, unknown>
    : null;
  if (typeof payload?.type === "string") {
    return payload.type;
  }
  const item = typeof parsed.item === "object" && parsed.item !== null
    ? parsed.item as Record<string, unknown>
    : null;
  return typeof item?.type === "string" ? item.type : null;
}

function rolloutTurnIdFromParsed(parsed: Record<string, unknown>): string | null {
  const payload = typeof parsed.payload === "object" && parsed.payload !== null
    ? parsed.payload as Record<string, unknown>
    : null;
  if (typeof payload?.turn_id === "string") {
    return payload.turn_id;
  }
  return typeof parsed.turn_id === "string" ? parsed.turn_id : null;
}

function rolloutEventId(rolloutPath: string, lineNumber: number, byteOffset: number): string {
  return `file:${createHash("sha256")
    .update(`${rolloutPath}:${lineNumber}:${byteOffset}`)
    .digest("hex")
    .slice(0, 32)}`;
}

function fileRolloutEventRow(
  rolloutPath: string,
  fallbackSessionId: string | null,
  lineNumber: number,
  byteOffset: number,
  byteLength: number,
  rawJson: string,
): RolloutEventRow {
  const fallbackTimestamp = new Date().toISOString();
  try {
    const parsed = JSON.parse(rawJson) as unknown;
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      const record = parsed as Record<string, unknown>;
      const timestamp = typeof record.timestamp === "string" ? record.timestamp : null;
      return {
        id: rolloutEventId(rolloutPath, lineNumber, byteOffset),
        sourceId: rolloutPath,
        absolutePath: rolloutPath,
        sessionId: fallbackSessionId,
        lineNumber,
        byteOffset,
        byteLength,
        timestamp,
        recordType: typeof record.type === "string" ? record.type : "unknown",
        eventType: rolloutEventTypeFromParsed(record),
        turnId: rolloutTurnIdFromParsed(record),
        payloadJson: record.payload ?? null,
        rawJson,
        createdAt: timestamp ?? fallbackTimestamp,
      };
    }
  } catch {
    // Keep raw invalid lines visible to tail consumers.
  }

  return {
    id: rolloutEventId(rolloutPath, lineNumber, byteOffset),
    sourceId: rolloutPath,
    absolutePath: rolloutPath,
    sessionId: fallbackSessionId,
    lineNumber,
    byteOffset,
    byteLength,
    timestamp: null,
    recordType: "invalid_json",
    eventType: null,
    turnId: null,
    payloadJson: null,
    rawJson,
    createdAt: fallbackTimestamp,
  };
}

async function listRolloutEventRowsFromFile(
  rolloutPath: string,
  input: RolloutEventListInput,
): Promise<RolloutEventRow[]> {
  const limit = Math.max(1, Math.min(Math.trunc(input.limit ?? 100), 500));
  const fallbackSessionId = input.sessionId ?? sessionIdFromRolloutPath(rolloutPath);
  const rows: RolloutEventRow[] = [];
  const stream = createReadStream(rolloutPath, { encoding: "utf8" });
  let pending = "";
  let pendingOffset = 0;
  let nextOffset = 0;
  let lineNumber = 0;

  for await (const chunk of stream) {
    let text = pending + chunk;
    let lineOffset = pendingOffset;
    pending = "";

    while (true) {
      const newline = text.indexOf("\n");
      if (newline === -1) {
        pending = text;
        pendingOffset = lineOffset;
        break;
      }

      const rawLine = text.slice(0, newline);
      const byteLength = Buffer.byteLength(rawLine, "utf8") + 1;
      lineNumber += 1;
      if (
        (input.afterLineNumber === undefined || lineNumber > input.afterLineNumber) &&
        (input.afterByteOffset === undefined || lineOffset > input.afterByteOffset)
      ) {
        rows.push(fileRolloutEventRow(
          rolloutPath,
          fallbackSessionId,
          lineNumber,
          lineOffset,
          byteLength,
          rawLine,
        ));
        if (rows.length >= limit) {
          stream.destroy();
          return rows;
        }
      }

      text = text.slice(newline + 1);
      lineOffset += byteLength;
    }

    nextOffset += Buffer.byteLength(chunk, "utf8");
    if (pending) {
      pendingOffset = nextOffset - Buffer.byteLength(pending, "utf8");
    }
  }

  if (pending) {
    lineNumber += 1;
    if (
      (input.afterLineNumber === undefined || lineNumber > input.afterLineNumber) &&
      (input.afterByteOffset === undefined || pendingOffset > input.afterByteOffset)
    ) {
      rows.push(fileRolloutEventRow(
        rolloutPath,
        fallbackSessionId,
        lineNumber,
        pendingOffset,
        Buffer.byteLength(pending, "utf8"),
        pending,
      ));
    }
  }

  return rows;
}

async function findRolloutPathBySessionId(codexHome: string, sessionId: string): Promise<string | null> {
  const expectedSuffix = `-${sessionId}.jsonl`;
  const sessionDate = dateFromUuidV7SessionId(sessionId);
  if (sessionDate) {
    for (const offset of [0, -1, 1]) {
      const day = new Date(sessionDate.getTime() + offset * 24 * 60 * 60 * 1000);
      const dayRoot = join(
        codexHome,
        "sessions",
        String(day.getUTCFullYear()),
        String(day.getUTCMonth() + 1).padStart(2, "0"),
        String(day.getUTCDate()).padStart(2, "0"),
      );
      const dayMatch = (await collectRolloutPathsUnder(dayRoot))
        .find((rolloutPath) => rolloutPath.endsWith(expectedSuffix));
      if (dayMatch) {
        return dayMatch;
      }
    }
  }

  const recentRolloutPaths = await collectRecentRolloutPaths(codexHome, 3);
  const recentMatch = recentRolloutPaths.find((rolloutPath) => rolloutPath.endsWith(expectedSuffix));
  if (recentMatch) {
    return recentMatch;
  }
  const rolloutPaths = await collectRolloutPathsUnder(join(codexHome, "sessions"));
  return rolloutPaths.find((rolloutPath) => rolloutPath.endsWith(expectedSuffix)) ?? null;
}

function dateFromUuidV7SessionId(sessionId: string): Date | null {
  const timestampHex = sessionId.replaceAll("-", "").slice(0, 12);
  if (!/^[0-9a-f]{12}$/i.test(timestampHex)) {
    return null;
  }
  const timestampMs = Number.parseInt(timestampHex, 16);
  if (!Number.isSafeInteger(timestampMs) || timestampMs <= 0) {
    return null;
  }
  const date = new Date(timestampMs);
  return Number.isNaN(date.getTime()) ? null : date;
}

async function resolveRolloutPathForEvents(
  store: ReturnType<typeof createCodexSessionStore>,
  codexHome: string,
  input: RolloutEventListInput,
): Promise<string | null> {
  if (input.absolutePath) {
    return input.absolutePath;
  }
  if (!input.sessionId) {
    return null;
  }
  const rolloutPath = await findRolloutPathBySessionId(codexHome, input.sessionId);
  if (rolloutPath) {
    return rolloutPath;
  }
  const session = await store.getSession(input.sessionId);
  if (session?.rollout_path) {
    return session.rollout_path;
  }
  return null;
}

async function listRolloutEventRows(
  store: ReturnType<typeof createCodexSessionStore>,
  codexHome: string,
  input: RolloutEventListInput,
): Promise<RolloutEventRow[]> {
  const rolloutPath = await resolveRolloutPathForEvents(store, codexHome, input);
  if (!rolloutPath) {
    return [];
  }
  return listRolloutEventRowsFromFile(rolloutPath, {
    ...input,
    absolutePath: rolloutPath,
  });
}

function textDeltaFromRolloutPayload(payload: unknown): string | undefined {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    return undefined;
  }
  const record = payload as Record<string, unknown>;
  return typeof record.delta === "string" ? record.delta : undefined;
}

function compactRolloutPayloadJson(row: RolloutEventRow): JsonValue | undefined {
  if (row.recordType === "session_meta" || row.recordType === "turn_context") {
    return undefined;
  }
  if (typeof row.payloadJson === "object" && row.payloadJson !== null) {
    const encoded = JSON.stringify(row.payloadJson);
    if (encoded.length > 16_384) {
      return {
        omitted: true,
        reason: "payload_too_large",
        byteLength: Buffer.byteLength(encoded, "utf8"),
      };
    }
  }
  return row.payloadJson as JsonValue;
}

function rolloutRowToStreamEventInput(
  row: RolloutEventRow,
  sourceHost: string,
): RecordCodexStreamEventInput | null {
  const sessionId = row.sessionId ?? sessionIdFromRolloutPath(row.absolutePath);
  if (!sessionId) {
    return null;
  }
  return {
    eventId: row.id,
    sessionId,
    turnId: row.turnId ?? undefined,
    sequence: row.lineNumber,
    eventKind: row.recordType,
    eventType: row.eventType ?? undefined,
    sourceId: row.sourceId,
    sourceHost,
    sourcePath: row.absolutePath,
    textDelta: textDeltaFromRolloutPayload(row.payloadJson),
    payloadJson: compactRolloutPayloadJson(row),
    rawJson: row.recordType === "invalid_json" ? row.rawJson : undefined,
    schemaHash: backendSchemaHash(),
    createdAt: row.createdAt,
    observedAt: new Date(),
  };
}

async function replicateRolloutEvents(options: {
  store: CodexSessionStoreHandle;
  codexHome: string;
  input: ReplicateRolloutEventsInput;
  signal?: AbortSignal;
  onEvent?: (event: CodexStreamEventRow) => void;
}): Promise<ReplicateRolloutEventsResult> {
  const rolloutPath = await resolveRolloutPathForEvents(
    options.store,
    options.codexHome,
    options.input,
  );
  if (!rolloutPath) {
    return {
      absolutePath: null,
      sessionId: options.input.sessionId ?? null,
      recorded: 0,
      lastLineNumber: options.input.afterLineNumber ?? null,
      lastByteOffset: options.input.afterByteOffset ?? null,
      followed: !!options.input.follow,
    };
  }

  const sourceHost = options.input.sourceHost?.trim() || hostname();
  let afterLineNumber = options.input.afterLineNumber;
  let afterByteOffset = options.input.afterByteOffset;
  let recorded = 0;
  let scannedBatches = 0;
  let hasMore = false;
  const maxBatches =
    options.input.maxBatches && options.input.maxBatches > 0
      ? Math.trunc(options.input.maxBatches)
      : null;

  const scanAvailableRows = async (): Promise<number> => {
    let batchRecorded = 0;
    while (!options.signal?.aborted) {
      if (maxBatches !== null && scannedBatches >= maxBatches) {
        hasMore = true;
        return batchRecorded;
      }
      const rows = await listRolloutEventRowsFromFile(rolloutPath, {
        ...options.input,
        absolutePath: rolloutPath,
        afterLineNumber,
        afterByteOffset,
        limit: options.input.limit ?? 200,
      });
      if (rows.length === 0) {
        return batchRecorded;
      }
      scannedBatches += 1;
      for (const row of rows) {
        const input = rolloutRowToStreamEventInput(row, sourceHost);
        afterLineNumber = row.lineNumber;
        afterByteOffset = row.byteOffset;
        if (!input) {
          continue;
        }
        const event = await options.store.recordCodexStreamEvent(input);
        recorded += 1;
        batchRecorded += 1;
        options.onEvent?.(event);
        if (options.input.yieldBetweenEvents) {
          await delay(0, options.signal);
        }
      }
      if (rows.length < Math.max(1, Math.min(Math.trunc(options.input.limit ?? 200), 500))) {
        return batchRecorded;
      }
    }
    return batchRecorded;
  };

  await scanAvailableRows();

  while (options.input.follow && !options.signal?.aborted) {
    const reason = await waitForRolloutChange({
      rolloutPath,
      pollIntervalMs: options.input.pollIntervalMs,
      idleTimeoutMs: options.input.idleTimeoutMs,
      signal: options.signal,
    });
    if (reason !== "changed") {
      break;
    }
    await scanAvailableRows();
  }

  return {
    absolutePath: rolloutPath,
    sessionId: options.input.sessionId ?? sessionIdFromRolloutPath(rolloutPath),
    recorded,
    lastLineNumber: afterLineNumber ?? null,
    lastByteOffset: afterByteOffset ?? null,
    followed: !!options.input.follow,
    hasMore,
  };
}

async function waitForRolloutChange(options: {
  rolloutPath: string;
  pollIntervalMs: number;
  idleTimeoutMs?: number;
  signal?: AbortSignal;
}): Promise<"changed" | "idle" | "aborted"> {
  if (options.signal?.aborted) {
    return "aborted";
  }

  let done = false;
  const controller = new AbortController();
  const abort = () => {
    done = true;
    controller.abort();
  };
  options.signal?.addEventListener("abort", abort, { once: true });

  const startStamp = await rolloutFileStamp(options.rolloutPath);
  try {
    const changed = Promise.race([
      waitForRolloutFsEvent(options.rolloutPath, controller.signal),
      waitForRolloutStatChange(
        options.rolloutPath,
        startStamp,
        Math.max(10, options.pollIntervalMs),
        () => done || !!options.signal?.aborted,
      ),
      waitForRolloutIdle(options.idleTimeoutMs, options.signal),
    ]);
    const result = await changed;
    return options.signal?.aborted ? "aborted" : result;
  } finally {
    abort();
    options.signal?.removeEventListener("abort", abort);
  }
}

async function waitForRolloutFsEvent(
  rolloutPath: string,
  signal: AbortSignal,
): Promise<"changed" | "aborted"> {
  const targetName = basename(rolloutPath);
  try {
    for await (const event of watchPath(dirname(rolloutPath), { signal })) {
      if (!event.filename || event.filename === targetName) {
        return "changed";
      }
    }
  } catch (error) {
    if (!(error instanceof Error) || error.name !== "AbortError") {
      return "changed";
    }
  }
  return "aborted";
}

async function waitForRolloutStatChange(
  rolloutPath: string,
  startStamp: string | null,
  pollIntervalMs: number,
  done: () => boolean,
): Promise<"changed" | "aborted"> {
  while (!done()) {
    await delay(pollIntervalMs);
    const currentStamp = await rolloutFileStamp(rolloutPath);
    if (currentStamp !== startStamp) {
      return "changed";
    }
  }
  return "aborted";
}

async function waitForRolloutIdle(
  idleTimeoutMs: number | undefined,
  signal: AbortSignal | undefined,
): Promise<"idle" | "aborted"> {
  if (idleTimeoutMs === undefined) {
    return new Promise<"idle" | "aborted">(() => undefined);
  }
  await delay(Math.max(0, idleTimeoutMs), signal);
  return signal?.aborted ? "aborted" : "idle";
}

async function rolloutFileStamp(rolloutPath: string): Promise<string | null> {
  const fileStat = await stat(rolloutPath).catch(() => null);
  return fileStat ? `${fileStat.size}:${fileStat.mtimeMs}` : null;
}

function toJAgentDefinitionRow(definition: JAgentDefinition): JAgentDefinitionRow {
  return {
    definitionId: definition.definition_id,
    name: definition.name,
    version: definition.version,
    sourceKind: definition.source_kind,
    entrypoint: definition.entrypoint,
    metadataJson: nullish(definition.metadata_json),
    createdAt: asIsoString(definition.created_at),
    updatedAt: asIsoString(definition.updated_at),
  };
}

function toJAgentRunRow(run: JAgentRun): JAgentRunRow {
  return {
    runId: run.run_id,
    definitionId: run.definition_id,
    status: run.status,
    projectRoot: run.project_root,
    repoRoot: nullish(run.repo_root),
    cwd: nullish(run.cwd),
    triggerSource: nullish(run.trigger_source),
    parentSessionId: nullish(run.parent_session_id),
    parentTurnId: nullish(run.parent_turn_id),
    initiatorSessionId: nullish(run.initiator_session_id),
    requestedRole: nullish(run.requested_role),
    requestedModel: nullish(run.requested_model),
    requestedReasoningEffort: nullish(run.requested_reasoning_effort),
    forkTurns: nullish(run.fork_turns),
    currentStepKey: nullish(run.current_step_key),
    inputJson: nullish(run.input_json),
    outputJson: nullish(run.output_json),
    errorText: nullish(run.error_text),
    startedAt: asIsoString(run.started_at),
    updatedAt: asIsoString(run.updated_at),
    completedAt: asNullableIsoString(run.completed_at),
  };
}

function toJAgentStepRow(step: JAgentStep): JAgentStepRow {
  return {
    stepId: step.step_id,
    runId: step.run_id,
    sequence: step.sequence,
    stepKey: step.step_key,
    stepKind: step.step_kind,
    status: step.status,
    inputJson: nullish(step.input_json),
    outputJson: nullish(step.output_json),
    errorText: nullish(step.error_text),
    startedAt: asIsoString(step.started_at),
    updatedAt: asIsoString(step.updated_at),
    completedAt: asNullableIsoString(step.completed_at),
  };
}

function toJAgentAttemptRow(attempt: JAgentAttempt): JAgentAttemptRow {
  return {
    attemptId: attempt.attempt_id,
    runId: attempt.run_id,
    stepId: attempt.step_id,
    attempt: attempt.attempt,
    status: attempt.status,
    codexSessionId: nullish(attempt.codex_session_id),
    codexTurnId: nullish(attempt.codex_turn_id),
    forkTurns: nullish(attempt.fork_turns),
    modelName: nullish(attempt.model_name),
    reasoningEffort: nullish(attempt.reasoning_effort),
    startedAt: asIsoString(attempt.started_at),
    completedAt: asNullableIsoString(attempt.completed_at),
    errorText: nullish(attempt.error_text),
  };
}

function toJAgentWaitRow(wait: JAgentWait): JAgentWaitRow {
  return {
    waitId: wait.wait_id,
    runId: wait.run_id,
    stepId: wait.step_id,
    waitKind: wait.wait_kind,
    targetSessionId: nullish(wait.target_session_id),
    targetTurnId: nullish(wait.target_turn_id),
    resumeConditionJson: nullish(wait.resume_condition_json),
    status: wait.status,
    startedAt: asIsoString(wait.started_at),
    resumedAt: asNullableIsoString(wait.resumed_at),
  };
}

function toJAgentSessionBindingRow(binding: JAgentSessionBinding): JAgentSessionBindingRow {
  return {
    bindingId: binding.binding_id,
    runId: binding.run_id,
    codexSessionId: binding.codex_session_id,
    bindingRole: binding.binding_role,
    parentSessionId: nullish(binding.parent_session_id),
    createdAt: asIsoString(binding.created_at),
  };
}

function toJAgentArtifactRow(artifact: JAgentArtifact): JAgentArtifactRow {
  return {
    artifactId: artifact.artifact_id,
    runId: artifact.run_id,
    stepId: nullish(artifact.step_id),
    kind: artifact.kind,
    path: artifact.path,
    textPreview: nullish(artifact.text_preview),
    metadataJson: nullish(artifact.metadata_json),
    createdAt: asIsoString(artifact.created_at),
  };
}

function toJAgentRunSummaryRow(summary: JAgentRunSummary): JAgentRunSummaryRow {
  return {
    definition: toJAgentDefinitionRow(summary.definition),
    run: toJAgentRunRow(summary.run),
    steps: summary.steps.map(toJAgentStepRow),
    attempts: summary.attempts.map(toJAgentAttemptRow),
    waits: summary.waits.map(toJAgentWaitRow),
    sessionBindings: summary.sessionBindings.map(toJAgentSessionBindingRow),
    artifacts: summary.artifacts.map(toJAgentArtifactRow),
    boundSessions: summary.boundSessions.map(toSessionReferenceRow),
  };
}

function withMatchRank(row: SessionLookupRow, matchRank: number | null): SessionCandidateRow {
  return { ...row, matchRank };
}

function tokenizeLookupQuery(query: string): string[] {
  return normalizeQuery(query).split(" ").filter(Boolean);
}

function splitSearchWords(value: string): string[] {
  return value
    .split(/[^a-z0-9]+/i)
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean);
}

function subsequencePenalty(field: string, token: string): number | null {
  if (token.length < 3 || field.length < token.length) {
    return null;
  }

  let tokenIndex = 0;
  let startIndex: number | null = null;
  let endIndex = 0;
  let skipped = 0;

  for (const [index, character] of [...field].entries()) {
    if (character !== token[tokenIndex]) {
      if (startIndex !== null) {
        skipped += 1;
      }
      continue;
    }

    if (startIndex === null) {
      startIndex = index;
    }
    endIndex = index;
    tokenIndex += 1;

    if (tokenIndex === token.length) {
      return 200 + (endIndex - startIndex) + skipped;
    }
  }

  return null;
}

function fieldTokenRank(field: string, token: string): number | null {
  const normalizedField = cleanText(field).toLowerCase();
  if (!normalizedField || !token) {
    return null;
  }

  if (normalizedField === token) {
    return 0;
  }
  if (normalizedField.startsWith(token)) {
    return 10;
  }

  const words = splitSearchWords(normalizedField);
  const exactWordIndex = words.findIndex((word) => word === token);
  if (exactWordIndex !== -1) {
    return 20 + exactWordIndex;
  }

  const prefixWordIndex = words.findIndex((word) => word.startsWith(token));
  if (prefixWordIndex !== -1) {
    return 40 + prefixWordIndex;
  }

  if (token.length >= 2) {
    const substringIndex = normalizedField.indexOf(token);
    if (substringIndex !== -1) {
      return 80 + substringIndex;
    }
  }

  return subsequencePenalty(normalizedField, token);
}

function sessionSearchRank(
  session: CodexSession,
  row: SessionLookupRow,
  tokens: string[],
): number | null {
  const fields: Array<{ value: string | undefined; weight: number }> = [
    { value: row.id, weight: 0 },
    { value: session.first_user_message, weight: 50 },
    { value: session.latest_user_message, weight: 75 },
    { value: session.latest_preview, weight: 100 },
    { value: session.latest_assistant_partial, weight: 125 },
    { value: session.latest_assistant_message, weight: 150 },
    { value: row.preview, weight: 175 },
    { value: row.branch, weight: 250 },
    { value: row.model, weight: 325 },
  ];

  let totalRank = 0;
  for (const token of tokens) {
    let bestRank: number | null = null;
    for (const field of fields) {
      const fieldRank = fieldTokenRank(field.value ?? "", token);
      if (fieldRank === null) {
        continue;
      }
      const weightedRank = field.weight + fieldRank;
      if (bestRank === null || weightedRank < bestRank) {
        bestRank = weightedRank;
      }
    }

    if (bestRank === null) {
      return null;
    }

    totalRank += bestRank;
  }

  return totalRank;
}

function searchSessions(
  sessions: CodexSession[],
  query: string,
  limit: number,
): SessionCandidateRow[] {
  const tokens = tokenizeLookupQuery(query);
  if (tokens.length === 0) {
    return [];
  }

  const rows = sessions
    .map((session) => ({ session, row: toSessionLookupRow(session) }))
    .map(({ session, row }) => {
      const rank = sessionSearchRank(session, row, tokens);
      return rank === null ? null : withMatchRank(row, rank);
    })
    .filter((row): row is SessionCandidateRow => row !== null)
    .sort((left, right) => {
      const leftRank = left.matchRank ?? Number.MAX_SAFE_INTEGER;
      const rightRank = right.matchRank ?? Number.MAX_SAFE_INTEGER;
      return leftRank - rightRank || right.updatedAt - left.updatedAt;
    });
  return rows.slice(0, limit);
}

function prefixSessions(sessions: CodexSession[], prefix: string, limit: number): SessionCandidateRow[] {
  const prefixNorm = prefix.trim().toLowerCase();
  const rows = sessions
    .map(toSessionLookupRow)
    .filter((row) => row.id.toLowerCase().startsWith(prefixNorm))
    .map((row) => withMatchRank(row, row.id.toLowerCase() === prefixNorm ? 0 : 1))
    .sort((left, right) => {
      const leftRank = left.matchRank ?? Number.MAX_SAFE_INTEGER;
      const rightRank = right.matchRank ?? Number.MAX_SAFE_INTEGER;
      return leftRank - rightRank || right.updatedAt - left.updatedAt;
    });
  return rows.slice(0, limit);
}

async function listActiveSessions(
  store: ReturnType<typeof createCodexSessionStore>,
  projectRoot: string | undefined,
  limit: number,
  catalogPrimer: CatalogPrimer,
  recentRolloutSyncScheduler?: RecentRolloutSyncScheduler,
): Promise<SessionLookupRow[]> {
  const normalizedProjectRoot = projectRoot ? expandHomePath(projectRoot) : undefined;
  const activeOptions = normalizedProjectRoot ? { projectRoot: normalizedProjectRoot, limit } : { limit };
  if (recentRolloutSyncScheduler) {
    await recentRolloutSyncScheduler.ensureFresh({
      limit: activeSessionFreshSyncLimit(limit),
      maxWaitMs: ACTIVE_SESSION_RECENT_SYNC_BUDGET_MS,
    });
  }
  let summaries = await store.listActiveSessionSummaries(activeOptions);
  if (summaries.length > 0) {
    return summaries.map((summary) => toSessionLookupRow(summary.session));
  }

  const [presenceRows, storedSessions] = normalizedProjectRoot
    ? await Promise.all([
        store.listSessionPresence({ projectRoot: normalizedProjectRoot, limit: 1 }),
        store.listSessionsForProjectRoot(normalizedProjectRoot, 1),
      ])
    : await Promise.all([
        store.listSessionPresence({ limit: 1 }),
        store.listSessions(1),
      ]);

  if (presenceRows.length === 0 && storedSessions.length > 0) {
    warmActiveSessionPresenceInBackground(store, normalizedProjectRoot);
    return [];
  }

  if (storedSessions.length === 0) {
    if (recentRolloutSyncScheduler) {
      void recentRolloutSyncScheduler.ensureFresh({
        limit: activeSessionFreshSyncLimit(limit),
      }).catch((error: unknown) => {
        logBackgroundError("recent active-session sync failed", error);
      });
    }
    warmCatalogInBackground(catalogPrimer);
    return [];
  }

  return summaries.map((summary) => toSessionLookupRow(summary.session));
}

function createSessionSyncScheduler(options: {
  store: ReturnType<typeof createCodexSessionStore>;
  codexHome: string;
  onProjectionSynced?: (event: SyncedProjectionEvent) => void;
}): SessionSyncScheduler {
  const inFlight = new Map<
    string,
    Promise<{ scanned: number; synced: number; found: boolean }>
  >();
  const pending = new Set<string>();

  const syncSession = async (
    sessionId: string,
  ): Promise<{ scanned: number; synced: number; found: boolean }> => {
    const activeSync = inFlight.get(sessionId);
    if (activeSync) {
      pending.add(sessionId);
      return activeSync;
    }

    const promise = (async () => {
      try {
        while (true) {
          pending.delete(sessionId);
          const result = await syncCodexSessionRollout({
            codexHome: options.codexHome,
            store: options.store,
            sessionId,
            onProjectionSynced: options.onProjectionSynced,
          });
          if (!pending.has(sessionId)) {
            return result;
          }
        }
      } finally {
        inFlight.delete(sessionId);
      }
    })();

    inFlight.set(sessionId, promise);
    return promise;
  };

  return { syncSession };
}

function createCatalogPrimer(options: {
  store: ReturnType<typeof createCodexSessionStore>;
  codexHome: string;
}): CatalogPrimer {
  let initialBackfillCompleted = false;
  let inFlight: Promise<void> | null = null;

  const ensurePrimed = async (): Promise<void> => {
    if (initialBackfillCompleted) {
      return;
    }
    if (inFlight) {
      return inFlight;
    }

    inFlight = (async () => {
      await syncCodexRollouts({
        codexHome: options.codexHome,
        store: options.store,
      });
      initialBackfillCompleted = true;
    })().finally(() => {
      inFlight = null;
    });

    return inFlight;
  };

  return { ensurePrimed };
}

function createRecentRolloutSyncScheduler(options: {
  store: ReturnType<typeof createCodexSessionStore>;
  codexHome: string;
}): RecentRolloutSyncScheduler {
  let inFlight: Promise<void> | null = null;
  let lastSyncedAt = 0;
  let pendingLimit = 0;

  const ensureFresh = async (request: RecentRolloutSyncRequest): Promise<void> => {
    const normalizedLimit = Math.max(
      1,
      Math.min(Math.trunc(request.limit), ACTIVE_SESSION_RECENT_SYNC_LIMIT),
    );
    pendingLimit = Math.max(pendingLimit, normalizedLimit);

    if (inFlight) {
      await waitForPromiseWithinBudget(inFlight, request.maxWaitMs);
      return;
    }

    const now = Date.now();
    if (lastSyncedAt !== 0 && now - lastSyncedAt < 750) {
      return;
    }

    const targetLimit = pendingLimit;
    pendingLimit = 0;
    const syncPromise = (async () => {
      await syncNewestCodexRollouts({
        codexHome: options.codexHome,
        store: options.store,
        limit: targetLimit,
      });
      lastSyncedAt = Date.now();
    })();
    syncPromise.catch((error: unknown) => {
      logBackgroundError("recent rollout sync failed", error);
    });
    inFlight = syncPromise.finally(() => {
      inFlight = null;
    });

    await waitForPromiseWithinBudget(inFlight, request.maxWaitMs);
  };

  return { ensureFresh };
}

async function withCatalogPrimeOnEmpty<T>(
  load: () => Promise<T>,
  isEmpty: (value: T) => boolean,
  catalogPrimer: CatalogPrimer,
): Promise<T> {
  const initial = await load();
  if (!isEmpty(initial)) {
    return initial;
  }

  await catalogPrimer.ensurePrimed();
  return load();
}

function warmCatalogInBackground(catalogPrimer: CatalogPrimer): void {
  void catalogPrimer.ensurePrimed().catch((error: unknown) => {
    logBackgroundError("catalog primer failed", error);
  });
}

async function loadRecentSessionsForProjectRoot(options: {
  store: ReturnType<typeof createCodexSessionStore>;
  codexHome: string;
  projectRoot: string;
  limit: number;
  catalogPrimer?: CatalogPrimer;
}): Promise<CodexSession[]> {
  const initial = await options.store.listSessionsForProjectRoot(options.projectRoot, options.limit);
  if (initial.length > 0) {
    if (options.catalogPrimer) {
      warmCatalogInBackground(options.catalogPrimer);
    }
    return initial;
  }

  await syncRecentSessionsForProjectRoot({
    codexHome: options.codexHome,
    store: options.store,
    projectRoot: options.projectRoot,
    limit: options.limit,
  });

  const loaded = await options.store.listSessionsForProjectRoot(options.projectRoot, options.limit);
  if (options.catalogPrimer) {
    warmCatalogInBackground(options.catalogPrimer);
  }
  return loaded;
}

async function loadRecentSessionsForProjectRootWithinBudget(options: {
  store: ReturnType<typeof createCodexSessionStore>;
  codexHome: string;
  projectRoot: string;
  limit: number;
  catalogPrimer: CatalogPrimer;
  maxWaitMs: number;
}): Promise<CodexSession[]> {
  const initial = await options.store.listSessionsForProjectRoot(options.projectRoot, options.limit);
  if (initial.length > 0) {
    warmCatalogInBackground(options.catalogPrimer);
    return initial;
  }

  const primePromise = loadRecentSessionsForProjectRoot({
    store: options.store,
    codexHome: options.codexHome,
    projectRoot: options.projectRoot,
    limit: options.limit,
  });
  const timedResult = await Promise.race([
    primePromise,
    new Promise<CodexSession[]>((resolve) => {
      setTimeout(() => resolve(initial), options.maxWaitMs);
    }),
  ]);

  void primePromise.catch((error: unknown) => {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(message);
  });
  warmCatalogInBackground(options.catalogPrimer);
  return timedResult;
}

async function loadSessionsByPrefix(options: {
  store: ReturnType<typeof createCodexSessionStore>;
  codexHome: string;
  prefix: string;
  limit: number;
  catalogPrimer?: CatalogPrimer;
}): Promise<CodexSession[]> {
  const initial = await options.store.listSessions(500);
  const initialMatches = prefixSessions(initial, options.prefix, options.limit);
  if (initialMatches.length > 0) {
    if (options.catalogPrimer) {
      warmCatalogInBackground(options.catalogPrimer);
    }
    return initial;
  }

  await syncSessionsByPrefix({
    codexHome: options.codexHome,
    store: options.store,
    prefix: options.prefix,
    limit: options.limit,
  });

  const loaded = await options.store.listSessions(500);
  if (options.catalogPrimer) {
    warmCatalogInBackground(options.catalogPrimer);
  }
  return loaded;
}

async function dispatchSessionServiceRequest(
  getRuntimeDeps: () => SessionServiceRuntimeDeps,
  getStreamStore: () => CodexSessionStoreHandle,
  request: SessionServiceRequest,
  dataPath: string,
  codexHome: string,
  runtimeInfo: SessionServiceRuntimeInfo,
): Promise<unknown> {
  if (request.method === "health") {
    return {
      status: "ok",
      pid: process.pid,
      schemaHash: backendSchemaHash(),
      dataPath,
      streamDataPath: runtimeInfo.streamDataPath,
      socketPath: runtimeInfo.socketPath,
      watchRollouts: runtimeInfo.watchRollouts,
      watchStreamRollouts: runtimeInfo.watchStreamRollouts,
      timestamp: new Date().toISOString(),
    };
  }
  if (request.method === "record-event") {
    return asStreamEventJsonLine(
      await getStreamStore().recordCodexStreamEvent(
        normalizeRecordCodexStreamEventInput(request.payload as Record<string, unknown>),
      ),
    );
  }
  if (request.method === "list-stream-events") {
    return (await getStreamStore().listCodexStreamEvents({
      sessionId: request.sessionId,
      turnId: request.turnId,
      afterSequence: request.afterSequence,
      limit: request.limit ?? 200,
      latest: request.latest,
    })).map((event) => asStreamEventJsonLine(event, {
      includePayload: request.includePayload !== false,
    }));
  }
  if (request.method === "replicate-rollout-events") {
    if (!request.sessionId && !request.absolutePath) {
      throw new Error("replicate-rollout-events requires sessionId or absolutePath");
    }
    if (request.follow) {
      throw new Error("replicate-rollout-events follow mode is only available from the CLI");
    }
    return replicateRolloutEvents({
      store: getStreamStore(),
      codexHome,
      input: {
        sessionId: request.sessionId,
        absolutePath: request.absolutePath ? expandHomePath(request.absolutePath) : undefined,
        afterLineNumber: request.afterLineNumber,
        afterByteOffset: request.afterByteOffset,
        limit: request.limit ?? 200,
        follow: false,
        pollIntervalMs: request.pollIntervalMs ?? 1000,
        idleTimeoutMs: request.idleTimeoutMs,
        sourceHost: request.sourceHost,
      },
    });
  }

  const {
    store,
    sessionSyncScheduler,
    catalogPrimer,
    recentRolloutSyncScheduler,
  } = getRuntimeDeps();

  switch (request.method) {
    case "list-sessions": {
      if (!request.projectRoot) {
        throw new Error("list-sessions requires projectRoot");
      }
      const projectRoot = expandHomePath(request.projectRoot);
      const sessions = await loadRecentSessionsForProjectRootWithinBudget({
        store,
        codexHome,
        projectRoot,
        limit: request.limit ?? 10,
        catalogPrimer,
        maxWaitMs: SESSION_SERVICE_PRIME_BUDGET_MS,
      });
      return sessions
        .map(toSessionLookupRow)
        .sort((left, right) => right.updatedAt - left.updatedAt);
    }
    case "search-sessions": {
      if (!request.query) {
        throw new Error("search-sessions requires query");
      }
      const projectRoot = request.projectRoot ? expandHomePath(request.projectRoot) : undefined;
      const sessions = await withCatalogPrimeOnEmpty(
        () => (projectRoot ? store.listSessionsForProjectRoot(projectRoot) : store.listSessions()),
        (rows) => rows.length === 0,
        catalogPrimer,
      );
      return searchSessions(sessions, request.query, request.limit ?? 5);
    }
    case "search-prefix-sessions": {
      if (!request.prefix) {
        throw new Error("search-prefix-sessions requires prefix");
      }
      const sessions = await loadSessionsByPrefix({
        store,
        codexHome,
        prefix: request.prefix,
        limit: request.limit ?? 5,
        catalogPrimer,
      });
      return prefixSessions(sessions, request.prefix, request.limit ?? 5);
    }
    case "get-session": {
      if (!request.sessionId) {
        throw new Error("get-session requires sessionId");
      }
      let session = await store.getSession(request.sessionId);
      if (!session) {
        const syncResult = await sessionSyncScheduler.syncSession(request.sessionId);
        if (syncResult.found) {
          session = await store.getSession(request.sessionId);
        }
      }
      return session ? toSessionLookupRow(session) : null;
    }
    case "sync-session": {
      if (!request.sessionId) {
        throw new Error("sync-session requires sessionId");
      }
      return sessionSyncScheduler.syncSession(request.sessionId);
    }
    case "list-active-sessions":
      return listActiveSessions(
        store,
        request.projectRoot,
        request.limit ?? 10,
        catalogPrimer,
        recentRolloutSyncScheduler,
      );
    case "list-active-runs": {
      const runs = await store.listActiveJAgentRuns(
        request.projectRoot
          ? { projectRoot: expandHomePath(request.projectRoot), limit: request.limit ?? 10 }
          : { limit: request.limit ?? 10 },
      );
      return runs.map(toJAgentRunRow);
    }
    case "list-runs-for-session": {
      if (!request.sessionId) {
        throw new Error("list-runs-for-session requires sessionId");
      }
      const runs = await store.listJAgentRunsForSession(request.sessionId, {
        limit: request.limit ?? 10,
      });
      return runs.map(toJAgentRunRow);
    }
    case "get-run-summary": {
      if (!request.runId) {
        throw new Error("get-run-summary requires runId");
      }
      const summary = await store.getJAgentRunSummary(request.runId);
      return summary ? toJAgentRunSummaryRow(summary) : null;
    }
    case "list-completions": {
      const completions = await store.listCompletionEvents({
        completedAfter: request.completedAfter,
        limit: request.limit ?? 50,
      });
      return completions.map(asJsonLine);
    }
    case "list-rollout-events": {
      if (!request.sessionId && !request.absolutePath) {
        throw new Error("list-rollout-events requires sessionId or absolutePath");
      }
      return listRolloutEventRows(store, codexHome, {
        sessionId: request.sessionId,
        absolutePath: request.absolutePath ? expandHomePath(request.absolutePath) : undefined,
        afterLineNumber: request.afterLineNumber,
        afterByteOffset: request.afterByteOffset,
        limit: request.limit ?? 100,
      });
    }
    case "replicate-rollout-events": {
      if (!request.sessionId && !request.absolutePath) {
        throw new Error("replicate-rollout-events requires sessionId or absolutePath");
      }
      if (request.follow) {
        throw new Error("replicate-rollout-events follow mode is only available from the CLI");
      }
      return replicateRolloutEvents({
        store,
        codexHome,
        input: {
          sessionId: request.sessionId,
          absolutePath: request.absolutePath ? expandHomePath(request.absolutePath) : undefined,
          afterLineNumber: request.afterLineNumber,
          afterByteOffset: request.afterByteOffset,
          limit: request.limit ?? 200,
          follow: false,
          pollIntervalMs: request.pollIntervalMs ?? 1000,
          idleTimeoutMs: request.idleTimeoutMs,
          sourceHost: request.sourceHost,
        },
      });
    }
    case "upsert-definition": {
      const input = normalizeDefinitionInput(
        request.payload as UpsertJAgentDefinitionInput,
      );
      const definition = await store.upsertJAgentDefinition(input);
      return toJAgentDefinitionRow(definition);
    }
    case "record-run-started": {
      const input = normalizeRunStartedInput(
        request.payload as RecordJAgentRunStartedInput,
      );
      const run = await store.recordJAgentRunStarted(input);
      return toJAgentRunRow(run);
    }
    case "record-terminal-presence": {
      const summary = await store.recordTerminalPresence(
        normalizeRecordCodexTerminalPresenceInput(
          request.payload as RecordCodexTerminalPresenceInput,
        ),
      );
      return summary;
    }
    case "record-run-completed": {
      const run = await store.recordJAgentRunCompleted(
        request.payload as RecordJAgentRunCompletedInput,
      );
      return toJAgentRunRow(run);
    }
    case "record-step-started": {
      const step = await store.recordJAgentStepStarted(
        request.payload as RecordJAgentStepStartedInput,
      );
      return toJAgentStepRow(step);
    }
    case "record-step-completed": {
      const step = await store.recordJAgentStepCompleted(
        request.payload as RecordJAgentStepCompletedInput,
      );
      return toJAgentStepRow(step);
    }
    case "record-attempt-started": {
      const attempt = await store.recordJAgentAttemptStarted(
        request.payload as RecordJAgentAttemptStartedInput,
      );
      return toJAgentAttemptRow(attempt);
    }
    case "record-attempt-completed": {
      const attempt = await store.recordJAgentAttemptCompleted(
        request.payload as RecordJAgentAttemptCompletedInput,
      );
      return toJAgentAttemptRow(attempt);
    }
    case "record-wait-started": {
      const wait = await store.recordJAgentWaitStarted(
        request.payload as RecordJAgentWaitStartedInput,
      );
      return toJAgentWaitRow(wait);
    }
    case "resolve-wait": {
      const wait = await store.recordJAgentWaitResolved(
        request.payload as RecordJAgentWaitResolvedInput,
      );
      return toJAgentWaitRow(wait);
    }
    case "bind-session": {
      const binding = await store.bindJAgentSession(
        request.payload as BindJAgentSessionInput,
      );
      return toJAgentSessionBindingRow(binding);
    }
    case "record-artifact": {
      const input = normalizeArtifactInput(
        request.payload as RecordJAgentArtifactInput,
      );
      const artifact = await store.recordJAgentArtifact(input);
      return toJAgentArtifactRow(artifact);
    }
    default:
      throw new Error(`Unsupported session service method: ${request.method}`);
  }
}

function cleanupSocketPath(socketPath: string): void {
  try {
    rmSync(socketPath, { force: true });
  } catch {
    // Ignore stale socket cleanup failures so startup and shutdown remain best-effort.
  }
}

function cleanupInstanceLock(lockPath: string): void {
  try {
    rmSync(lockPath, { force: true });
  } catch {
    // Ignore stale lock cleanup failures so startup and shutdown remain best-effort.
  }
}

function lockPathForSocket(socketPath: string): string {
  return `${socketPath}.lock`;
}

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return !processIsZombie(pid);
  } catch (error) {
    const code =
      typeof error === "object" && error !== null && "code" in error
        ? String((error as { code?: unknown }).code ?? "")
        : "";
    return code === "EPERM";
  }
}

function processIsZombie(pid: number): boolean {
  if (process.platform === "win32") {
    return false;
  }

  try {
    const result = spawnSync("ps", ["-o", "stat=", "-p", String(pid)], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
    if (result.status !== 0) {
      return false;
    }
    const status = result.stdout.trim();
    return status.toUpperCase().includes("Z");
  } catch {
    return false;
  }
}

function readSessionServiceLockMetadata(lockPath: string): SessionServiceLockMetadata | null {
  try {
    const parsed = JSON.parse(readFileSync(lockPath, "utf8")) as Partial<SessionServiceLockMetadata>;
    if (
      typeof parsed.pid === "number" &&
      Number.isFinite(parsed.pid) &&
      typeof parsed.socketPath === "string" &&
      parsed.socketPath.length > 0 &&
      typeof parsed.startedAt === "string" &&
      parsed.startedAt.length > 0
    ) {
      return {
        pid: parsed.pid,
        socketPath: parsed.socketPath,
        startedAt: parsed.startedAt,
      };
    }
  } catch {
    // Treat malformed or unreadable lock contents as stale.
  }
  return null;
}

async function canConnectToSocket(socketPath: string): Promise<boolean> {
  return await new Promise((resolve) => {
    const socket = createConnection({ path: socketPath });
    let settled = false;

    const finish = (result: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(result);
    };

    socket.once("connect", () => {
      finish(true);
    });
    socket.once("error", () => {
      finish(false);
    });
    socket.setTimeout(250, () => {
      finish(false);
    });
  });
}

async function canQuerySessionServiceHealth(socketPath: string): Promise<boolean> {
  return await new Promise((resolve) => {
    const socket = createConnection({ path: socketPath });
    let settled = false;
    let buffer = "";

    const finish = (result: boolean) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(result);
    };

    socket.setEncoding("utf8");
    socket.once("connect", () => {
      socket.write(`${JSON.stringify({ id: "health-probe", method: "health" })}\n`);
    });
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex === -1) {
        return;
      }
      const line = buffer.slice(0, newlineIndex).trim();
      if (!line) {
        finish(false);
        return;
      }
      try {
        const response = JSON.parse(line) as {
          ok?: unknown;
          result?: { status?: unknown };
        };
        finish(response.ok === true && response.result?.status === "ok");
      } catch {
        finish(false);
      }
    });
    socket.once("error", () => {
      finish(false);
    });
    socket.setTimeout(500, () => {
      finish(false);
    });
  });
}

async function waitForSocketReady(socketPath: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await canQuerySessionServiceHealth(socketPath)) {
      return true;
    }
    await delay(50);
  }
  return await canQuerySessionServiceHealth(socketPath);
}

function isSessionServiceLockStale(metadata: SessionServiceLockMetadata): boolean {
  const startedAtMs = Date.parse(metadata.startedAt);
  if (!Number.isFinite(startedAtMs)) {
    return false;
  }
  return Date.now() - startedAtMs >= SESSION_SERVICE_STALE_LOCK_GRACE_MS;
}

async function acquireSessionServiceLock(socketPath: string): Promise<SessionServiceInstanceLock> {
  const lockPath = lockPathForSocket(socketPath);
  const metadata: SessionServiceLockMetadata = {
    pid: process.pid,
    socketPath,
    startedAt: new Date().toISOString(),
  };

  await mkdir(dirname(lockPath), { recursive: true });

  while (true) {
    try {
      writeFileSync(lockPath, `${JSON.stringify(metadata)}\n`, {
        encoding: "utf8",
        flag: "wx",
      });
      return { lockPath, metadata };
    } catch (error) {
      const code =
        typeof error === "object" && error !== null && "code" in error
          ? String((error as { code?: unknown }).code ?? "")
          : "";
      if (code !== "EEXIST") {
        throw error;
      }
    }

    const existing = readSessionServiceLockMetadata(lockPath);
    if (!existing) {
      cleanupInstanceLock(lockPath);
      if (!(await canConnectToSocket(socketPath))) {
        cleanupSocketPath(socketPath);
      }
      continue;
    }
    if (!processExists(existing.pid)) {
      cleanupInstanceLock(lockPath);
      if (!(await canConnectToSocket(socketPath))) {
        cleanupSocketPath(socketPath);
      }
      continue;
    }
    if (await waitForSocketReady(socketPath, SESSION_SERVICE_STARTUP_WAIT_MS)) {
      throw new Error(`session service already running at ${socketPath}`);
    }
    if (!processExists(existing.pid)) {
      cleanupInstanceLock(lockPath);
      if (!(await canConnectToSocket(socketPath))) {
        cleanupSocketPath(socketPath);
      }
      continue;
    }
    if (isSessionServiceLockStale(existing)) {
      console.error(
        `warning: reclaiming stale Jazz2 session service lock for pid ${existing.pid} at ${socketPath}`,
      );
      cleanupInstanceLock(lockPath);
      if (!(await canConnectToSocket(socketPath))) {
        cleanupSocketPath(socketPath);
      }
      continue;
    }
    throw new Error(
      `session service lock is held by pid ${existing.pid}, but ${socketPath} never became ready`,
    );
  }
}

async function prepareSocketPath(socketPath: string): Promise<void> {
  const alreadyServing = await canQuerySessionServiceHealth(socketPath);
  if (alreadyServing) {
    throw new Error(`session service already running at ${socketPath}`);
  }
  cleanupSocketPath(socketPath);
}

function writeSessionServiceResponse(socket: Socket, response: SessionServiceResponse): void {
  if (socket.destroyed) {
    return;
  }
  socket.write(`${JSON.stringify(response)}\n`);
}

function summarizeSessionServiceRequest(request: SessionServiceRequest): string {
  const details = [
    request.sessionId ? `sessionId=${request.sessionId}` : null,
    request.projectRoot ? `projectRoot=${request.projectRoot}` : null,
    request.prefix ? `prefix=${request.prefix}` : null,
    request.runId ? `runId=${request.runId}` : null,
  ].filter((value): value is string => !!value);

  return details.length > 0
    ? `${request.method} (${details.join(", ")})`
    : request.method;
}

async function dispatchSessionServiceRequestWithTimeout(
  getRuntimeDeps: () => SessionServiceRuntimeDeps,
  getStreamStore: () => CodexSessionStoreHandle,
  request: SessionServiceRequest,
  dataPath: string,
  codexHome: string,
  runtimeInfo: SessionServiceRuntimeInfo,
): Promise<unknown> {
  const timeoutSentinel = Symbol("session-service-timeout");
  const result = await Promise.race([
    dispatchSessionServiceRequest(
      getRuntimeDeps,
      getStreamStore,
      request,
      dataPath,
      codexHome,
      runtimeInfo,
    ),
    delay(SESSION_SERVICE_REQUEST_TIMEOUT_MS).then(() => timeoutSentinel),
  ]);

  if (result === timeoutSentinel) {
    throw new Error(
      `Jazz2 session service request timed out after ${SESSION_SERVICE_REQUEST_TIMEOUT_MS}ms: ${summarizeSessionServiceRequest(request)}`,
    );
  }

  return result;
}

function handleSessionServiceConnection(
  socket: Socket,
  getRuntimeDeps: () => SessionServiceRuntimeDeps,
  getStreamStore: () => CodexSessionStoreHandle,
  dataPath: string,
  codexHome: string,
  completionBroadcaster: CompletionBroadcaster,
  runtimeInfo: SessionServiceRuntimeInfo,
): void {
  socket.setEncoding("utf8");
  let buffer = "";
  let processing = false;

  const pumpRequests = async (): Promise<void> => {
    if (processing) {
      return;
    }
    processing = true;
    try {
      while (true) {
        const newlineIndex = buffer.indexOf("\n");
        if (newlineIndex === -1) {
          return;
        }

        const rawLine = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);

        if (!rawLine) {
          continue;
        }

        let request: SessionServiceRequest;
        try {
          request = JSON.parse(rawLine) as SessionServiceRequest;
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          writeSessionServiceResponse(socket, {
            ok: false,
            error: `invalid JSON request: ${message}`,
          });
          continue;
        }

        if (request.method === "watch-completions") {
          completionBroadcaster.subscribe(socket, request.id, {
            completedAfter: request.completedAfter,
            limit: request.limit,
          });
          return;
        }

        try {
          const result = await dispatchSessionServiceRequestWithTimeout(
            getRuntimeDeps,
            getStreamStore,
            request,
            dataPath,
            codexHome,
            runtimeInfo,
          );
          writeSessionServiceResponse(socket, {
            id: request.id,
            ok: true,
            result,
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          writeSessionServiceResponse(socket, {
            id: request.id,
            ok: false,
            error: message,
          });
        }
      }
    } finally {
      processing = false;
      if (!socket.destroyed && buffer.includes("\n")) {
        void pumpRequests();
      }
    }
  };

  socket.on("data", (chunk: string) => {
    buffer += chunk;
    void pumpRequests();
  });

  socket.on("error", () => {
    socket.destroy();
  });
}

async function serveSessionQueries(options: {
  getStore: () => CodexSessionStoreHandle;
  getStreamStore: () => CodexSessionStoreHandle;
  socketPath: string;
  dataPath: string;
  streamDataPath: string;
  codexHome: string;
  pollIntervalMs: number;
  watchRollouts: boolean;
  watchStreamRollouts: boolean;
  warmStreamStore: boolean;
}): Promise<void> {
  await mkdir(dirname(options.socketPath), { recursive: true });
  const instanceLock = await acquireSessionServiceLock(options.socketPath);
  const completionBroadcaster = new CompletionBroadcaster();
  let runtimeDeps: SessionServiceRuntimeDeps | null = null;
  const getRuntimeDeps = (): SessionServiceRuntimeDeps => {
    if (runtimeDeps) {
      return runtimeDeps;
    }

    const store = options.getStore();
    runtimeDeps = {
      store,
      sessionSyncScheduler: createSessionSyncScheduler({
        store,
        codexHome: options.codexHome,
        onProjectionSynced: ({ completionEvents }) => {
          if (completionEvents.length > 0) {
            completionBroadcaster.publish(completionEvents);
          }
        },
      }),
      catalogPrimer: createCatalogPrimer({
        store,
        codexHome: options.codexHome,
      }),
      recentRolloutSyncScheduler: createRecentRolloutSyncScheduler({
        store,
        codexHome: options.codexHome,
      }),
    };
    return runtimeDeps;
  };

  const server = createServer((socket) => {
    handleSessionServiceConnection(
      socket,
      getRuntimeDeps,
      options.getStreamStore,
      options.dataPath,
      options.codexHome,
      completionBroadcaster,
      {
        socketPath: options.socketPath,
        watchRollouts: options.watchRollouts,
        watchStreamRollouts: options.watchStreamRollouts,
        streamDataPath: options.streamDataPath,
      },
    );
  });

  const stop = async () => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  };

  const watchController = new AbortController();
  const stopWatch = () => watchController.abort();
  process.once("SIGINT", stopWatch);
  process.once("SIGTERM", stopWatch);

  let watchPromise: Promise<void> | null = null;
  const handleWatchError = (label: string) => (error: unknown) => {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(`[${label}] ${message}`);
  };

  try {
    await prepareSocketPath(options.socketPath);
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.socketPath, () => {
        server.off("error", reject);
        resolve();
      });
    });
    if (options.warmStreamStore) {
      void options.getStreamStore()
        .listCodexStreamEvents({ limit: 1 })
        .catch(handleWatchError("warm-stream-store"));
    }

    const promises: Promise<void>[] = [];
    if (options.watchStreamRollouts) {
      const streamRolloutPromise = (async () => {
        const streamPollIntervalMs = Math.max(1000, options.pollIntervalMs);
        await delay(streamPollIntervalMs, watchController.signal);
        await watchRecentRolloutStreamEvents({
          codexHome: options.codexHome,
          store: options.getStreamStore(),
          pollIntervalMs: streamPollIntervalMs,
          signal: watchController.signal,
        });
      })().catch(handleWatchError("watch-stream-rollouts"));
      promises.push(streamRolloutPromise);
    }

    const shouldWatchRolloutsInBackground =
      process.env.FLOW_CODEX_SESSION_BACKGROUND_ROLLOUT_WATCH === "1" ||
      process.env.FLOW_CODEX_SESSION_BACKGROUND_ROLLOUT_WATCH === "true";
    if (options.watchRollouts && shouldWatchRolloutsInBackground) {
      const syncPromise = watchCodexRollouts({
        codexHome: options.codexHome,
        store: getRuntimeDeps().store,
        pollIntervalMs: options.pollIntervalMs,
        recentScanLimit: 16,
        fullRescanEveryMs: 24 * 60 * 60 * 1000,
        onProjectionSynced: ({ completionEvents }) => {
          if (completionEvents.length > 0) {
            completionBroadcaster.publish(completionEvents);
          }
        },
        signal: watchController.signal,
      }).catch(handleWatchError("watch-rollouts"));
      promises.push(syncPromise);
    }

    const shouldWatchFileCompletions = process.env.FLOW_CODEX_SESSION_FILE_COMPLETION_WATCH === "1"
      || process.env.FLOW_CODEX_SESSION_FILE_COMPLETION_WATCH === "true";
    if (shouldWatchFileCompletions) {
      const completionPromise = watchRecentCompletionEvents({
        codexHome: options.codexHome,
        pollIntervalMs: options.pollIntervalMs,
        bootstrapWindowMs: 10 * 60 * 1000,
        signal: watchController.signal,
        onEvents: (completionEvents) => {
          if (completionEvents.length > 0) {
            completionBroadcaster.publish(completionEvents);
          }
        },
      }).catch(handleWatchError("watch-completions"));
      promises.push(completionPromise);
    }
    watchPromise = Promise.all(promises).then(() => undefined);

    await new Promise<void>((resolve) => {
      const shutdown = async () => {
        stopWatch();
        await stop().catch(() => undefined);
        resolve();
      };

      process.once("SIGINT", () => {
        void shutdown();
      });
      process.once("SIGTERM", () => {
        void shutdown();
      });
    });
  } finally {
    stopWatch();
    cleanupSocketPath(options.socketPath);
    cleanupInstanceLock(instanceLock.lockPath);
    if (watchPromise) {
      await watchPromise;
    }
  }
}

async function watchCompletionEvents(options: {
  codexHome: string;
  storeConfig: CodexSessionStoreConfig;
  pollIntervalMs: number;
  bootstrapWindowMs: number;
}): Promise<void> {
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);

  const store = createCodexSessionStore(options.storeConfig);
  const emittedIds = new Set<string>();
  const emittedOrder: string[] = [];
  let completedAfter = new Date(Date.now() - options.bootstrapWindowMs);
  let projectionError: unknown;

  const projectionPromise = watchCodexRollouts({
    codexHome: options.codexHome,
    store,
    pollIntervalMs: options.pollIntervalMs,
    signal: controller.signal,
  }).catch((error: unknown) => {
    projectionError = error;
    controller.abort();
  });

  try {
    while (!controller.signal.aborted) {
      const completions = await store.listCompletionEvents({
        completedAfter,
        limit: 100,
      });
      for (const completion of completions) {
        if (emittedIds.has(completion.id)) {
          continue;
        }
        process.stdout.write(`${JSON.stringify(asJsonLine(completion))}\n`);
        trackEmittedId(emittedIds, emittedOrder, completion.id);
      }
      const latestCompletion = completions.at(-1);
      if (latestCompletion) {
        completedAfter = latestCompletion.completedAt;
      }
      await delay(options.pollIntervalMs, controller.signal);
    }
    if (projectionError) {
      throw projectionError;
    }
  } finally {
    controller.abort();
    await projectionPromise;
    await store.shutdown();
  }
}

function asJsonLine(completion: CodexCompletionEvent): Record<string, unknown> {
  return {
    id: completion.id,
    source: completion.source,
    sessionId: completion.sessionId,
    turnId: completion.turnId,
    projectPath: completion.projectPath,
    projectName: completion.projectName,
    summary: completion.summary,
    status: completion.status,
    timestamp: completion.timestamp,
    completedAt: completion.completedAt,
    updatedAt: completion.updatedAt,
  };
}

function asStreamEventJsonLine(
  event: CodexStreamEventRow,
  options: { includePayload?: boolean } = {},
): Record<string, unknown> {
  const line: Record<string, unknown> = {
    id: event.id,
    eventId: event.event_id,
    sessionId: event.session_id,
    turnId: event.turn_id,
    sequence: event.sequence,
    eventKind: event.event_kind,
    eventType: event.event_type,
    sourceId: event.source_id,
    sourceHost: event.source_host,
    sourcePath: event.source_path,
    textDelta: event.text_delta,
    schemaHash: event.schema_hash,
    createdAt: event.created_at.toISOString(),
    observedAt: event.observed_at.toISOString(),
  };
  if (options.includePayload !== false) {
    line.payloadJson = event.payload_json;
    line.rawJson = event.raw_json;
  }
  return line;
}

async function watchStreamEvents(options: {
  store: CodexSessionStoreHandle;
  sessionId: string;
  turnId?: string;
  afterSequence?: number;
  limit: number;
  pollIntervalMs: number;
}): Promise<void> {
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);

  let afterSequence = options.afterSequence;
  const emittedIds = new Set<string>();

  try {
    while (!controller.signal.aborted) {
      const events = await options.store.listCodexStreamEvents({
        sessionId: options.sessionId,
        turnId: options.turnId,
        afterSequence,
        limit: options.limit,
      });
      for (const event of events) {
        if (emittedIds.has(event.event_id)) {
          continue;
        }
        emittedIds.add(event.event_id);
        afterSequence = Math.max(afterSequence ?? -1, event.sequence);
        process.stdout.write(`${JSON.stringify(asStreamEventJsonLine(event))}\n`);
      }
      await delay(options.pollIntervalMs, controller.signal);
    }
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
  }
}

interface CompletionSubscriber {
  requestId?: string;
  socket: Socket;
  completedAfter?: Date;
  limit: number;
}

class CompletionBroadcaster {
  private readonly subscribers = new Set<CompletionSubscriber>();
  private readonly recentEvents: Record<string, unknown>[] = [];
  private readonly seenIds = new Set<string>();
  private readonly seenOrder: string[] = [];

  constructor(
    private readonly historyLimit = 200,
    private readonly seenLimit = 2000,
  ) {}

  subscribe(
    socket: Socket,
    requestId: string | undefined,
    options?: { completedAfter?: string; limit?: number },
  ): void {
    const completedAfter = options?.completedAfter
      ? new Date(options.completedAfter)
      : undefined;
    const limit = Math.max(1, Math.min(options?.limit ?? 50, this.historyLimit));
    const subscriber: CompletionSubscriber = {
      requestId,
      socket,
      completedAfter:
        completedAfter && !Number.isNaN(completedAfter.getTime())
          ? completedAfter
          : undefined,
      limit,
    };

    writeSessionServiceResponse(socket, {
      id: requestId,
      ok: true,
      result: { status: "subscribed" },
    });

    const bootstrapEvents = this.bootstrapEvents(subscriber);
    for (const event of bootstrapEvents) {
      writeSessionServiceResponse(socket, {
        id: requestId,
        ok: true,
        event,
      });
    }

    this.subscribers.add(subscriber);
    const unsubscribe = () => {
      this.subscribers.delete(subscriber);
    };
    socket.once("close", unsubscribe);
    socket.once("end", unsubscribe);
    socket.once("error", unsubscribe);
  }

  publish(events: CodexCompletionEvent[]): void {
    const sorted = [...events].sort(
      (left, right) => left.completedAt.getTime() - right.completedAt.getTime(),
    );

    for (const event of sorted) {
      if (this.seenIds.has(event.id)) {
        continue;
      }
      this.trackSeenId(event.id);
      const payload = asJsonLine(event);
      this.recentEvents.push(payload);
      if (this.recentEvents.length > this.historyLimit) {
        this.recentEvents.splice(0, this.recentEvents.length - this.historyLimit);
      }

      for (const subscriber of [...this.subscribers]) {
        if (subscriber.completedAfter) {
          const completedAt = event.completedAt.getTime();
          if (completedAt < subscriber.completedAfter.getTime()) {
            continue;
          }
        }
        writeSessionServiceResponse(subscriber.socket, {
          id: subscriber.requestId,
          ok: true,
          event: payload,
        });
      }
    }
  }

  private bootstrapEvents(subscriber: CompletionSubscriber): Record<string, unknown>[] {
    let events = this.recentEvents;
    if (subscriber.completedAfter) {
      const cutoff = subscriber.completedAfter.getTime();
      events = events.filter((event) => {
        const completedAt =
          event.completedAt instanceof Date
            ? event.completedAt
            : typeof event.completedAt === "string"
              ? new Date(event.completedAt)
              : null;
        return completedAt ? completedAt.getTime() >= cutoff : false;
      });
    }
    if (events.length <= subscriber.limit) {
      return events;
    }
    return events.slice(events.length - subscriber.limit);
  }

  private trackSeenId(id: string): void {
    this.seenIds.add(id);
    this.seenOrder.push(id);
    if (this.seenOrder.length <= this.seenLimit) {
      return;
    }
    const staleCount = this.seenOrder.length - Math.floor(this.seenLimit * 0.75);
    for (const staleId of this.seenOrder.splice(0, staleCount)) {
      this.seenIds.delete(staleId);
    }
  }
}

interface CompletionRolloutWatcherOptions {
  codexHome: string;
  pollIntervalMs: number;
  bootstrapWindowMs: number;
  signal?: AbortSignal;
  onEvents: (events: CodexCompletionEvent[]) => void;
}

interface StreamRolloutWatcherOptions {
  codexHome: string;
  store: CodexSessionStoreHandle;
  pollIntervalMs: number;
  signal?: AbortSignal;
  sourceHost?: string;
}

interface StreamRolloutCursor {
  mtimeMs: number;
  lastLineNumber?: number;
  lastByteOffset?: number;
  hasBacklog?: boolean;
}

interface RecentRolloutPathStat {
  path: string;
  mtimeMs: number;
}

function streamWatchRolloutLimit(): number {
  const parsed = Number(process.env.FLOW_CODEX_SESSION_STREAM_WATCH_ROLLOUT_LIMIT ?? "24");
  return Math.max(1, Math.min(128, Math.trunc(Number.isFinite(parsed) ? parsed : 24)));
}

function streamWatchDayCount(): number {
  const parsed = Number(process.env.FLOW_CODEX_SESSION_STREAM_WATCH_DAYS ?? "2");
  return Math.max(1, Math.min(14, Math.trunc(Number.isFinite(parsed) ? parsed : 2)));
}

function streamWatchBatchLimit(): number {
  const parsed = Number(process.env.FLOW_CODEX_SESSION_STREAM_WATCH_BATCH_LIMIT ?? "5");
  return Math.max(1, Math.min(5000, Math.trunc(Number.isFinite(parsed) ? parsed : 5)));
}

function streamWatchBootstrapMode(): "tail" | "backfill" {
  const raw = (process.env.FLOW_CODEX_SESSION_STREAM_WATCH_BOOTSTRAP_MODE ?? "tail")
    .trim()
    .toLowerCase();
  return raw === "backfill" ? "backfill" : "tail";
}

async function collectRecentlyModifiedRolloutPaths(
  codexHome: string,
  dayCount: number,
  limit: number,
): Promise<RecentRolloutPathStat[]> {
  const paths = await collectRecentRolloutPaths(codexHome, dayCount, {
    recursive: false,
  });
  const rows = await Promise.all(
    paths.map(async (path): Promise<RecentRolloutPathStat | null> => {
      const fileStat = await stat(path).catch(() => null);
      if (!fileStat?.isFile()) {
        return null;
      }
      return { path, mtimeMs: fileStat.mtimeMs };
    }),
  );
  return rows
    .filter((row): row is RecentRolloutPathStat => row !== null)
    .sort((left, right) => right.mtimeMs - left.mtimeMs)
    .slice(0, limit);
}

async function watchRecentRolloutStreamEvents(
  options: StreamRolloutWatcherOptions,
): Promise<void> {
  const cursors = new Map<string, StreamRolloutCursor>();
  const sourceHost = options.sourceHost?.trim() || hostname();
  const bootstrapMode = streamWatchBootstrapMode();

  while (!options.signal?.aborted) {
    const rolloutPaths = await collectRecentlyModifiedRolloutPaths(
      options.codexHome,
      streamWatchDayCount(),
      streamWatchRolloutLimit(),
    );

    for (const rolloutPath of rolloutPaths) {
      if (options.signal?.aborted) {
        return;
      }
      await delay(0, options.signal);
      const cursor = cursors.get(rolloutPath.path);
      if (cursor && !cursor.hasBacklog && rolloutPath.mtimeMs <= cursor.mtimeMs) {
        continue;
      }
      if (!cursor && bootstrapMode === "tail") {
        const fileStat = await stat(rolloutPath.path).catch(() => null);
        cursors.set(rolloutPath.path, {
          mtimeMs: rolloutPath.mtimeMs,
          lastByteOffset: fileStat?.isFile() && fileStat.size > 0 ? fileStat.size - 1 : undefined,
        });
        continue;
      }

      const result = await replicateRolloutEvents({
        store: options.store,
        codexHome: options.codexHome,
        signal: options.signal,
        input: {
          absolutePath: rolloutPath.path,
          afterLineNumber: cursor?.lastLineNumber,
          afterByteOffset: cursor?.lastByteOffset,
          limit: streamWatchBatchLimit(),
          maxBatches: 1,
          follow: false,
          pollIntervalMs: options.pollIntervalMs,
          sourceHost,
          yieldBetweenEvents: true,
        },
      });

      cursors.set(rolloutPath.path, {
        mtimeMs: rolloutPath.mtimeMs,
        lastLineNumber: result.lastLineNumber ?? cursor?.lastLineNumber,
        lastByteOffset: result.lastByteOffset ?? cursor?.lastByteOffset,
        hasBacklog: !!result.hasMore,
      });
    }

    await delay(options.pollIntervalMs, options.signal);
  }
}

function completionWatchRolloutLimit(): number {
  const parsed = Number(process.env.FLOW_CODEX_SESSION_COMPLETION_WATCH_ROLLOUT_LIMIT ?? "8");
  return Math.max(1, Math.min(64, Math.trunc(Number.isFinite(parsed) ? parsed : 8)));
}

async function watchRecentCompletionEvents(
  options: CompletionRolloutWatcherOptions,
): Promise<void> {
  const seenRolloutMtimes = new Map<string, number>();
  const emittedIds = new Set<string>();
  const emittedOrder: string[] = [];
  const bootstrapCutoff = Date.now() - options.bootstrapWindowMs;

  while (!options.signal?.aborted) {
    const rolloutPaths = await collectRecentRolloutPaths(options.codexHome, 2, {
      limit: completionWatchRolloutLimit(),
      recursive: false,
    });
    for (const rolloutPath of rolloutPaths) {
      if (options.signal?.aborted) {
        return;
      }
      await delay(0, options.signal);
      const fileStat = await stat(rolloutPath).catch(() => null);
      if (!fileStat?.isFile()) {
        continue;
      }

      const previousMtime = seenRolloutMtimes.get(rolloutPath);
      const currentMtime = fileStat.mtimeMs;
      if (previousMtime !== undefined && currentMtime <= previousMtime) {
        continue;
      }
      seenRolloutMtimes.set(rolloutPath, currentMtime);

      const rolloutText = await readFile(rolloutPath, "utf8").catch(() => null);
      if (!rolloutText) {
        continue;
      }
      const built = buildSessionProjectionFromRollout(rolloutPath, rolloutText);
      if (!built) {
        continue;
      }

      const completionEvents = collectRecentCompletionEvents({
        projection: built.projection,
        previousMtime,
        bootstrapCutoff,
        emittedIds,
      });
      if (completionEvents.length > 0) {
        options.onEvents(completionEvents);
        for (const event of completionEvents) {
          trackEmittedId(emittedIds, emittedOrder, event.id);
        }
      }
    }

    await delay(options.pollIntervalMs, options.signal);
  }
}

interface RecentRolloutPathOptions {
  limit?: number;
  recursive?: boolean;
}

async function collectRecentRolloutPaths(
  codexHome: string,
  dayCount: number,
  options: RecentRolloutPathOptions = {},
): Promise<string[]> {
  const paths: string[] = [];
  const now = new Date();
  const limit = options.limit && options.limit > 0 ? Math.trunc(options.limit) : null;
  const recursive = options.recursive ?? true;

  for (let offset = 0; offset < dayCount; offset += 1) {
    const day = new Date(now.getTime() - offset * 24 * 60 * 60 * 1000);
    const dayRoot = join(
      codexHome,
      "sessions",
      String(day.getFullYear()),
      String(day.getMonth() + 1).padStart(2, "0"),
      String(day.getDate()).padStart(2, "0"),
    );
    const dayPaths = recursive
      ? await collectRolloutPathsUnder(dayRoot)
      : await collectRolloutPathsInDirectory(dayRoot);
    paths.push(...dayPaths);
    if (limit !== null && paths.length > limit * 2) {
      paths.sort();
      paths.splice(0, paths.length - limit);
    }
  }

  const sorted = paths.sort();
  return limit === null ? sorted : sorted.slice(-limit);
}

async function collectRolloutPathsInDirectory(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  return entries
    .filter((entry) => entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl"))
    .map((entry) => join(root, entry.name));
}

async function collectRolloutPathsUnder(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  const paths: string[] = [];

  for (const entry of entries) {
    const absolutePath = join(root, entry.name);
    if (entry.isDirectory()) {
      paths.push(...(await collectRolloutPathsUnder(absolutePath)));
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      paths.push(absolutePath);
    }
  }

  return paths;
}

async function main(): Promise<void> {
  const command = process.argv[2] ?? "sync";
  if (command === "help" || command === "--help" || command === "-h") {
    printUsage();
    return;
  }
  if (command === "schema-hash") {
    console.log(JSON.stringify({ schemaHash: backendSchemaHash() }));
    return;
  }

  const codexHome = expandHomePath(
    readFlag("--codex-home")
      ?? process.env.FLOW_CODEX_JAZZ_CODEX_HOME
      ?? process.env.J_SESSIONS_JAZZ_CODEX_HOME
      ?? "~/.codex",
  );
  const dataPath = await resolvePersistentDataPath(
    readFlag("--data-path")
      ?? process.env.FLOW_CODEX_JAZZ_DATA_PATH
      ?? process.env.J_SESSIONS_JAZZ_DATA_PATH
      ?? defaultJazzDataPath(),
  );
  const storeConfig = readStoreConfig(dataPath);
  const streamDataPath = await resolvePersistentDataPath(
    readFlag("--stream-data-path")
      ?? process.env.FLOW_CODEX_JAZZ_STREAM_DATA_PATH
      ?? process.env.J_SESSIONS_JAZZ_STREAM_DATA_PATH
      ?? defaultStreamDataPath(dataPath),
  );
  const streamStoreConfig = localStreamStoreConfig(storeConfig, streamDataPath);
  const socketPath = expandHomePath(
    readFlag("--socket-path")
      ?? process.env.FLOW_CODEX_JAZZ_SOCKET_PATH
      ?? process.env.J_SESSIONS_JAZZ_SOCKET_PATH
      ?? defaultJazzSocketPath(),
  );
  const pollIntervalMs = Number(readFlag("--poll-interval-ms") ?? "1000");
  const bootstrapWindowMs = Number(readFlag("--bootstrap-window-ms") ?? "15000");
  const watchRollouts = readBooleanFlag("--watch-rollouts", command == "serve" ? false : true);
  const watchStreamRollouts = readBooleanFlag("--watch-stream-rollouts", command === "serve");
  const warmStreamStore = readBooleanFlag("--warm-stream-store", false);

  if (command === "watch-completions") {
    await watchCompletionEvents({
      codexHome,
      storeConfig,
      pollIntervalMs,
      bootstrapWindowMs,
    });
    return;
  }

  if (command === "serve") {
    const storeRef: { current?: CodexSessionStoreHandle } = {};
    const streamStoreRef: { current?: CodexSessionStoreHandle } = {};
    const getStore = (): CodexSessionStoreHandle => {
      if (!storeRef.current) {
        storeRef.current = createCodexSessionStore(storeConfig);
      }
      return storeRef.current;
    };
    const getStreamStore = (): CodexSessionStoreHandle => {
      if (!streamStoreRef.current) {
        streamStoreRef.current = createCodexSessionStore(streamStoreConfig);
      }
      return streamStoreRef.current;
    };

    try {
      await serveSessionQueries({
        getStore,
        getStreamStore,
        socketPath,
        dataPath,
        streamDataPath,
        codexHome,
        pollIntervalMs,
        watchRollouts,
        watchStreamRollouts,
        warmStreamStore,
      });
      return;
    } finally {
      await Promise.allSettled([
        storeRef.current?.shutdown(),
        streamStoreRef.current?.shutdown(),
      ]);
    }
  }

  if (
    command === "record-event"
    || command === "list-stream-events"
    || command === "watch-stream-events"
    || command === "replicate-rollout-events"
  ) {
    const streamStore = createCodexSessionStore(streamStoreConfig);
    try {
      if (command === "record-event") {
        const input = normalizeRecordCodexStreamEventInput(
          readJsonInput<Record<string, unknown>>(command),
        );
        const event = await streamStore.recordCodexStreamEvent(input);
        console.log(JSON.stringify(asStreamEventJsonLine(event)));
        return;
      }

      if (command === "list-stream-events") {
        const includePayload = readBooleanFlag("--include-payload", true);
        const events = await streamStore.listCodexStreamEvents({
          sessionId: readFlag("--session-id"),
          turnId: readFlag("--turn-id"),
          afterSequence: readOptionalNumberFlag("--after-sequence"),
          limit: readOptionalNumberFlag("--limit") ?? 200,
          latest: readBooleanFlag("--latest", false),
        });
        console.log(JSON.stringify(events.map((event) => asStreamEventJsonLine(event, {
          includePayload,
        }))));
        return;
      }

      if (command === "watch-stream-events") {
        const sessionId = readFlag("--session-id");
        if (!sessionId) {
          throw new Error("watch-stream-events requires --session-id");
        }
        await watchStreamEvents({
          store: streamStore,
          sessionId,
          turnId: readFlag("--turn-id"),
          afterSequence: readOptionalNumberFlag("--after-sequence"),
          limit: readOptionalNumberFlag("--limit") ?? 200,
          pollIntervalMs,
        });
        return;
      }

      const sessionId = readFlag("--session-id");
      const absolutePath = readFlag("--absolute-path");
      if (!sessionId && !absolutePath) {
        throw new Error("replicate-rollout-events requires --session-id or --absolute-path");
      }
      const follow = readBooleanFlag("--follow", false);
      const controller = new AbortController();
      const stop = () => controller.abort();
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);
      try {
        const result = await replicateRolloutEvents({
          store: streamStore,
          codexHome,
          signal: controller.signal,
          input: {
            sessionId,
            absolutePath: absolutePath ? expandHomePath(absolutePath) : undefined,
            afterLineNumber: readOptionalNumberFlag("--after-line-number"),
            afterByteOffset: readOptionalNumberFlag("--after-byte-offset"),
            limit: readOptionalNumberFlag("--limit") ?? 200,
            follow,
            idleTimeoutMs: readOptionalNumberFlag("--idle-timeout-ms"),
            pollIntervalMs,
            sourceHost: readFlag("--source-host"),
          },
          onEvent: follow
            ? (event) => {
              process.stdout.write(`${JSON.stringify(asStreamEventJsonLine(event))}\n`);
            }
            : undefined,
        });
        if (!follow) {
          console.log(JSON.stringify(result));
        }
      } finally {
        process.off("SIGINT", stop);
        process.off("SIGTERM", stop);
      }
      return;
    } finally {
      await streamStore.shutdown();
    }
  }

  const store = createCodexSessionStore(storeConfig);

  try {
    if (command === "sync-session") {
      const sessionId = readFlag("--session-id");
      if (!sessionId) {
        throw new Error("sync-session requires --session-id");
      }
      const result = await syncCodexSessionRollout({ codexHome, store, sessionId });
      console.log(JSON.stringify({ ...result, sessionId, codexHome, dataPath }));
      return;
    }

    if (command === "list-sessions") {
      const projectRoot = readFlag("--project-root");
      if (!projectRoot) {
        throw new Error("list-sessions requires --project-root");
      }
      const limit = Number(readFlag("--limit") ?? "10");
      const sessions = await loadRecentSessionsForProjectRoot({
        store,
        codexHome,
        projectRoot: expandHomePath(projectRoot),
        limit,
      });
      console.log(
        JSON.stringify(
          sessions
            .map(toSessionLookupRow)
            .sort((left, right) => right.updatedAt - left.updatedAt),
        ),
      );
      return;
    }

    if (command === "list-completions") {
      const completedAfter = readFlag("--completed-after");
      const limit = Number(readFlag("--limit") ?? "50");
      const completions = await store.listCompletionEvents({
        completedAfter,
        limit,
      });
      console.log(JSON.stringify(completions.map(asJsonLine)));
      return;
    }

    if (command === "search-sessions") {
      const projectRoot = readFlag("--project-root");
      const query = readFlag("--query");
      if (!query) {
        throw new Error("search-sessions requires --query");
      }
      const limit = Number(readFlag("--limit") ?? "5");
      const sessions = await withCatalogPrimeOnEmpty(
        () =>
          projectRoot
            ? store.listSessionsForProjectRoot(expandHomePath(projectRoot))
            : store.listSessions(),
        (rows) => rows.length === 0,
        {
          ensurePrimed: async () => {
            await syncCodexRollouts({ codexHome, store });
          },
        },
      );
      console.log(JSON.stringify(searchSessions(sessions, query, limit)));
      return;
    }

    if (command === "search-prefix-sessions") {
      const prefix = readFlag("--prefix");
      if (!prefix) {
        throw new Error("search-prefix-sessions requires --prefix");
      }
      const limit = Number(readFlag("--limit") ?? "5");
      const sessions = await loadSessionsByPrefix({
        store,
        codexHome,
        prefix,
        limit,
      });
      console.log(JSON.stringify(prefixSessions(sessions, prefix, limit)));
      return;
    }

    if (command === "get-session") {
      const sessionId = readFlag("--session-id");
      if (!sessionId) {
        throw new Error("get-session requires --session-id");
      }
      let session = await store.getSession(sessionId);
      if (!session) {
        const syncResult = await syncCodexSessionRollout({ codexHome, store, sessionId });
        if (syncResult.found) {
          session = await store.getSession(sessionId);
        }
      }
      console.log(JSON.stringify(session ? toSessionLookupRow(session) : null));
      return;
    }

    if (command === "list-rollout-events") {
      const sessionId = readFlag("--session-id");
      const absolutePath = readFlag("--absolute-path");
      if (!sessionId && !absolutePath) {
        throw new Error("list-rollout-events requires --session-id or --absolute-path");
      }
      const events = await listRolloutEventRows(store, codexHome, {
        sessionId,
        absolutePath: absolutePath ? expandHomePath(absolutePath) : undefined,
        afterLineNumber: readOptionalNumberFlag("--after-line-number"),
        afterByteOffset: readOptionalNumberFlag("--after-byte-offset"),
        limit: readOptionalNumberFlag("--limit") ?? 100,
      });
      console.log(JSON.stringify(events));
      return;
    }

    if (command === "replicate-rollout-events") {
      const sessionId = readFlag("--session-id");
      const absolutePath = readFlag("--absolute-path");
      if (!sessionId && !absolutePath) {
        throw new Error("replicate-rollout-events requires --session-id or --absolute-path");
      }
      const follow = readBooleanFlag("--follow", false);
      const controller = new AbortController();
      const stop = () => controller.abort();
      process.once("SIGINT", stop);
      process.once("SIGTERM", stop);
      try {
        const result = await replicateRolloutEvents({
          store,
          codexHome,
          signal: controller.signal,
          input: {
            sessionId,
            absolutePath: absolutePath ? expandHomePath(absolutePath) : undefined,
            afterLineNumber: readOptionalNumberFlag("--after-line-number"),
            afterByteOffset: readOptionalNumberFlag("--after-byte-offset"),
            limit: readOptionalNumberFlag("--limit") ?? 200,
            follow,
            idleTimeoutMs: readOptionalNumberFlag("--idle-timeout-ms"),
            pollIntervalMs,
            sourceHost: readFlag("--source-host"),
          },
          onEvent: follow
            ? (event) => {
              process.stdout.write(`${JSON.stringify(asStreamEventJsonLine(event))}\n`);
            }
            : undefined,
        });
        if (!follow) {
          console.log(JSON.stringify(result));
        }
      } finally {
        process.off("SIGINT", stop);
        process.off("SIGTERM", stop);
      }
      return;
    }

    if (command === "list-stream-events") {
      const includePayload = readBooleanFlag("--include-payload", true);
      const events = await store.listCodexStreamEvents({
        sessionId: readFlag("--session-id"),
        turnId: readFlag("--turn-id"),
        afterSequence: readOptionalNumberFlag("--after-sequence"),
        limit: readOptionalNumberFlag("--limit") ?? 200,
        latest: readBooleanFlag("--latest", false),
      });
      console.log(JSON.stringify(events.map((event) => asStreamEventJsonLine(event, {
        includePayload,
      }))));
      return;
    }

    if (command === "watch-stream-events") {
      const sessionId = readFlag("--session-id");
      if (!sessionId) {
        throw new Error("watch-stream-events requires --session-id");
      }
      await watchStreamEvents({
        store,
        sessionId,
        turnId: readFlag("--turn-id"),
        afterSequence: readOptionalNumberFlag("--after-sequence"),
        limit: readOptionalNumberFlag("--limit") ?? 200,
        pollIntervalMs,
      });
      return;
    }

    if (command === "list-active-sessions") {
      const limit = Number(readFlag("--limit") ?? "10");
      const projectRoot = readFlag("--project-root");
      const sessions = await listActiveSessions(
        store,
        projectRoot,
        limit,
        {
          ensurePrimed: async () => {
            await syncCodexRollouts({ codexHome, store });
          },
        },
      );
      console.log(JSON.stringify(sessions));
      return;
    }

    if (command === "list-active-runs") {
      const limit = Number(readFlag("--limit") ?? "10");
      const projectRoot = readFlag("--project-root");
      const runs = await store.listActiveJAgentRuns(
        projectRoot ? { projectRoot: expandHomePath(projectRoot), limit } : { limit },
      );
      console.log(JSON.stringify(runs.map(toJAgentRunRow)));
      return;
    }

    if (command === "list-runs-for-session") {
      const sessionId = readFlag("--session-id");
      if (!sessionId) {
        throw new Error("list-runs-for-session requires --session-id");
      }
      const limit = Number(readFlag("--limit") ?? "10");
      const runs = await store.listJAgentRunsForSession(sessionId, { limit });
      console.log(JSON.stringify(runs.map(toJAgentRunRow)));
      return;
    }

    if (command === "get-run-summary") {
      const runId = readFlag("--run-id");
      if (!runId) {
        throw new Error("get-run-summary requires --run-id");
      }
      const summary = await store.getJAgentRunSummary(runId);
      console.log(JSON.stringify(summary ? toJAgentRunSummaryRow(summary) : null));
      return;
    }

    if (command === "upsert-definition") {
      const input = normalizeDefinitionInput(readJsonInput<UpsertJAgentDefinitionInput>(command));
      const definition = await store.upsertJAgentDefinition(input);
      console.log(JSON.stringify(toJAgentDefinitionRow(definition)));
      return;
    }

    if (command === "record-run-started") {
      const input = normalizeRunStartedInput(readJsonInput<RecordJAgentRunStartedInput>(command));
      const run = await store.recordJAgentRunStarted(input);
      console.log(JSON.stringify(toJAgentRunRow(run)));
      return;
    }

    if (command === "record-terminal-presence") {
      const input = normalizeRecordCodexTerminalPresenceInput(
        readJsonInput<RecordCodexTerminalPresenceInput>(command),
      );
      const summary = await store.recordTerminalPresence(input);
      console.log(JSON.stringify(summary));
      return;
    }

    if (command === "record-event") {
      const input = normalizeRecordCodexStreamEventInput(
        readJsonInput<Record<string, unknown>>(command),
      );
      const event = await store.recordCodexStreamEvent(input);
      console.log(JSON.stringify(asStreamEventJsonLine(event)));
      return;
    }

    if (command === "record-run-completed") {
      const input = readJsonInput<RecordJAgentRunCompletedInput>(command);
      const run = await store.recordJAgentRunCompleted(input);
      console.log(JSON.stringify(toJAgentRunRow(run)));
      return;
    }

    if (command === "record-step-started") {
      const input = readJsonInput<RecordJAgentStepStartedInput>(command);
      const step = await store.recordJAgentStepStarted(input);
      console.log(JSON.stringify(toJAgentStepRow(step)));
      return;
    }

    if (command === "record-step-completed") {
      const input = readJsonInput<RecordJAgentStepCompletedInput>(command);
      const step = await store.recordJAgentStepCompleted(input);
      console.log(JSON.stringify(toJAgentStepRow(step)));
      return;
    }

    if (command === "record-attempt-started") {
      const input = readJsonInput<RecordJAgentAttemptStartedInput>(command);
      const attempt = await store.recordJAgentAttemptStarted(input);
      console.log(JSON.stringify(toJAgentAttemptRow(attempt)));
      return;
    }

    if (command === "record-attempt-completed") {
      const input = readJsonInput<RecordJAgentAttemptCompletedInput>(command);
      const attempt = await store.recordJAgentAttemptCompleted(input);
      console.log(JSON.stringify(toJAgentAttemptRow(attempt)));
      return;
    }

    if (command === "record-wait-started") {
      const input = readJsonInput<RecordJAgentWaitStartedInput>(command);
      const wait = await store.recordJAgentWaitStarted(input);
      console.log(JSON.stringify(toJAgentWaitRow(wait)));
      return;
    }

    if (command === "resolve-wait") {
      const input = readJsonInput<RecordJAgentWaitResolvedInput>(command);
      const wait = await store.recordJAgentWaitResolved(input);
      console.log(JSON.stringify(toJAgentWaitRow(wait)));
      return;
    }

    if (command === "bind-session") {
      const input = readJsonInput<BindJAgentSessionInput>(command);
      const binding = await store.bindJAgentSession(input);
      console.log(JSON.stringify(toJAgentSessionBindingRow(binding)));
      return;
    }

    if (command === "record-artifact") {
      const input = normalizeArtifactInput(readJsonInput<RecordJAgentArtifactInput>(command));
      const artifact = await store.recordJAgentArtifact(input);
      console.log(JSON.stringify(toJAgentArtifactRow(artifact)));
      return;
    }

    if (command === "watch") {
      const controller = new AbortController();
      process.once("SIGINT", () => controller.abort());
      process.once("SIGTERM", () => controller.abort());
      await watchCodexRollouts({
        codexHome,
        store,
        pollIntervalMs,
        signal: controller.signal,
        onCycle: ({ scanned, synced }) => {
          console.log(JSON.stringify({ scanned, synced, codexHome, dataPath }));
        },
      });
      return;
    }

    const result = await syncCodexRollouts({ codexHome, store });
    console.log(JSON.stringify({ ...result, codexHome, dataPath }));
  } finally {
    await store.shutdown();
  }
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});

import { spawnSync } from "node:child_process";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { mkdir, readFile, readdir, stat } from "node:fs/promises";
import { createConnection, createServer, type Socket } from "node:net";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import type {
  CodexSession,
  JAgentArtifact,
  JAgentAttempt,
  JAgentDefinition,
  JAgentRun,
  JAgentSessionBinding,
  JAgentStep,
  JAgentWait,
} from "../schema/app.js";
import {
  type BindJAgentSessionInput,
  createCodexSessionStore,
  type CodexCompletionEvent,
  type JAgentRunSummary,
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
  completionEventsFromProjection,
  type SyncedProjectionEvent,
  syncCodexRollouts,
  syncCodexSessionRollout,
  syncNewestCodexRollouts,
  syncRecentSessionsForProjectRoot,
  syncSessionsByPrefix,
  watchCodexRollouts,
} from "./projector.js";

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

interface SessionServiceRequest {
  id?: string;
  method: string;
  projectRoot?: string;
  query?: string;
  prefix?: string;
  sessionId?: string;
  runId?: string;
  completedAfter?: string;
  limit?: number;
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

function trackEmittedId(ids: Set<string>, order: string[], id: string): void {
  ids.add(id);
  order.push(id);
  if (order.length <= 512) {
    return;
  }
  const staleIds = order.splice(0, order.length - 320);
  for (const staleId of staleIds) {
    ids.delete(staleId);
  }
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
  store: ReturnType<typeof createCodexSessionStore>,
  request: SessionServiceRequest,
  dataPath: string,
  codexHome: string,
  sessionSyncScheduler: SessionSyncScheduler,
  catalogPrimer: CatalogPrimer,
  recentRolloutSyncScheduler: RecentRolloutSyncScheduler,
  runtimeInfo: SessionServiceRuntimeInfo,
): Promise<unknown> {
  switch (request.method) {
    case "health":
      return {
        status: "ok",
        pid: process.pid,
        dataPath,
        socketPath: runtimeInfo.socketPath,
        watchRollouts: runtimeInfo.watchRollouts,
        timestamp: new Date().toISOString(),
      };
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

async function waitForSocketReady(socketPath: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await canConnectToSocket(socketPath)) {
      return true;
    }
    await delay(50);
  }
  return await canConnectToSocket(socketPath);
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
  const alreadyServing = await canConnectToSocket(socketPath);
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

function handleSessionServiceConnection(
  socket: Socket,
  store: ReturnType<typeof createCodexSessionStore>,
  dataPath: string,
  codexHome: string,
  sessionSyncScheduler: SessionSyncScheduler,
  catalogPrimer: CatalogPrimer,
  recentRolloutSyncScheduler: RecentRolloutSyncScheduler,
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
          const result = await dispatchSessionServiceRequest(
            store,
            request,
            dataPath,
            codexHome,
            sessionSyncScheduler,
            catalogPrimer,
            recentRolloutSyncScheduler,
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
  store: ReturnType<typeof createCodexSessionStore>;
  socketPath: string;
  dataPath: string;
  codexHome: string;
  pollIntervalMs: number;
  watchRollouts: boolean;
}): Promise<void> {
  await mkdir(dirname(options.socketPath), { recursive: true });
  const instanceLock = await acquireSessionServiceLock(options.socketPath);
  const completionBroadcaster = new CompletionBroadcaster();
  const sessionSyncScheduler = createSessionSyncScheduler({
    store: options.store,
    codexHome: options.codexHome,
    onProjectionSynced: ({ completionEvents }) => {
      if (completionEvents.length > 0) {
        completionBroadcaster.publish(completionEvents);
      }
    },
  });
  const catalogPrimer = createCatalogPrimer({
    store: options.store,
    codexHome: options.codexHome,
  });
  const recentRolloutSyncScheduler = createRecentRolloutSyncScheduler({
    store: options.store,
    codexHome: options.codexHome,
  });

  const server = createServer((socket) => {
    handleSessionServiceConnection(
      socket,
      options.store,
      options.dataPath,
      options.codexHome,
      sessionSyncScheduler,
      catalogPrimer,
      recentRolloutSyncScheduler,
      completionBroadcaster,
      {
        socketPath: options.socketPath,
        watchRollouts: options.watchRollouts,
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
    const promises: Promise<void>[] = [];
    if (options.watchRollouts) {
      const syncPromise = watchCodexRollouts({
        codexHome: options.codexHome,
        store: options.store,
        pollIntervalMs: options.pollIntervalMs,
        onProjectionSynced: ({ completionEvents }) => {
          if (completionEvents.length > 0) {
            completionBroadcaster.publish(completionEvents);
          }
        },
        signal: watchController.signal,
      }).catch(handleWatchError("watch-rollouts"));
      promises.push(syncPromise);
    }

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
    watchPromise = Promise.all(promises).then(() => undefined);

    await prepareSocketPath(options.socketPath);
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.socketPath, () => {
        server.off("error", reject);
        resolve();
      });
    });

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
  dataPath: string;
  pollIntervalMs: number;
  bootstrapWindowMs: number;
}): Promise<void> {
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);

  const store = createCodexSessionStore({ dataPath: options.dataPath });
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

async function watchRecentCompletionEvents(
  options: CompletionRolloutWatcherOptions,
): Promise<void> {
  const seenRolloutMtimes = new Map<string, number>();
  const bootstrapCutoff = Date.now() - options.bootstrapWindowMs;

  while (!options.signal?.aborted) {
    const rolloutPaths = await collectRecentRolloutPaths(options.codexHome, 2);
    for (const rolloutPath of rolloutPaths) {
      if (options.signal?.aborted) {
        return;
      }
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

      const completionEvents = completionEventsFromProjection(built.projection).filter((event) => {
        if (previousMtime !== undefined) {
          return event.completedAt.getTime() >= previousMtime;
        }
        return event.completedAt.getTime() >= bootstrapCutoff;
      });
      if (completionEvents.length > 0) {
        options.onEvents(completionEvents);
      }
    }

    await delay(options.pollIntervalMs, options.signal);
  }
}

async function collectRecentRolloutPaths(codexHome: string, dayCount: number): Promise<string[]> {
  const paths: string[] = [];
  const now = new Date();

  for (let offset = 0; offset < dayCount; offset += 1) {
    const day = new Date(now.getTime() - offset * 24 * 60 * 60 * 1000);
    const dayRoot = join(
      codexHome,
      "sessions",
      String(day.getFullYear()),
      String(day.getMonth() + 1).padStart(2, "0"),
      String(day.getDate()).padStart(2, "0"),
    );
    paths.push(...(await collectRolloutPathsUnder(dayRoot)));
  }

  return paths.sort();
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
  const socketPath = expandHomePath(
    readFlag("--socket-path")
      ?? process.env.FLOW_CODEX_JAZZ_SOCKET_PATH
      ?? process.env.J_SESSIONS_JAZZ_SOCKET_PATH
      ?? defaultJazzSocketPath(),
  );
  const pollIntervalMs = Number(readFlag("--poll-interval-ms") ?? "1000");
  const bootstrapWindowMs = Number(readFlag("--bootstrap-window-ms") ?? "15000");
  const watchRollouts = readBooleanFlag("--watch-rollouts", command == "serve" ? false : true);

  if (command === "watch-completions") {
    await watchCompletionEvents({
      codexHome,
      dataPath,
      pollIntervalMs,
      bootstrapWindowMs,
    });
    return;
  }

  const store = createCodexSessionStore({ dataPath });

  try {
    if (command === "serve") {
      await serveSessionQueries({
        store,
        socketPath,
        dataPath,
        codexHome,
        pollIntervalMs,
        watchRollouts,
      });
      return;
    }

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
      const session = await store.getSession(sessionId);
      console.log(JSON.stringify(session ? toSessionLookupRow(session) : null));
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

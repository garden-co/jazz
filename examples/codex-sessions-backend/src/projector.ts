import { readdir, readFile, stat } from "node:fs/promises";
import { basename, join } from "node:path";
import type {
  CodexCompletionEvent,
  CodexSessionProjection,
  CodexSessionStore,
  CodexTurnProjection,
} from "./store.js";

type ProjectionJsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: ProjectionJsonValue }
  | ProjectionJsonValue[];

type RolloutPayload = Record<string, unknown>;

interface RolloutLine {
  timestamp?: string;
  type: string;
  payload?: RolloutPayload;
}

interface MutableTurn {
  turnId: string;
  sequence: number;
  status: string;
  userMessage?: string;
  lastUserAt?: Date;
  assistantSegments: string[];
  assistantPartial?: string;
  lastAssistantAt?: Date;
  planText?: string;
  reasoningSummary?: string;
  startedAt?: Date;
  completedAt?: Date;
  durationMs?: number;
  updatedAt: Date;
}

interface BuiltSessionProjection {
  projection: CodexSessionProjection;
  lineCount: number;
}

export interface SyncCodexRolloutsOptions {
  codexHome: string;
  store: CodexSessionStore;
  signal?: AbortSignal;
  onProjectionSynced?: (event: SyncedProjectionEvent) => void;
}

export interface SyncCodexSessionRolloutOptions extends SyncCodexRolloutsOptions {
  sessionId: string;
}

export interface SyncRecentProjectSessionsOptions extends SyncCodexRolloutsOptions {
  projectRoot: string;
  limit: number;
}

export interface SyncSessionPrefixOptions extends SyncCodexRolloutsOptions {
  prefix: string;
  limit: number;
}

export interface SyncNewestCodexRolloutsOptions extends SyncCodexRolloutsOptions {
  limit: number;
}

export interface WatchCodexRolloutsOptions extends SyncCodexRolloutsOptions {
  pollIntervalMs?: number;
  onCycle?: (result: { scanned: number; synced: number }) => void;
  recentScanLimit?: number;
  fullRescanEveryMs?: number;
}

export interface SyncedProjectionEvent {
  rolloutPath: string;
  syncedAt: Date;
  lineCount: number;
  projection: CodexSessionProjection;
  completionEvents: CodexCompletionEvent[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function getNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

function parseIsoDate(value: unknown): Date | undefined {
  const text = getString(value);
  if (!text) {
    return undefined;
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

function parseUnixSeconds(value: unknown): Date | undefined {
  const seconds = getNumber(value);
  if (seconds === undefined) {
    return undefined;
  }
  return new Date(seconds * 1000);
}

function coerceDate(value: Date | string | number): Date {
  return value instanceof Date ? value : new Date(value);
}

function latestTurnDate(
  turns: CodexTurnProjection[],
  predicate: (turn: CodexTurnProjection) => boolean,
  selectDate: (turn: CodexTurnProjection) => Date | string | number | undefined,
): Date | undefined {
  const matchedTurn = [...turns].reverse().find(predicate);
  const value = matchedTurn ? selectDate(matchedTurn) : undefined;
  return value === undefined ? undefined : coerceDate(value);
}

function appendText(existing: string | undefined, next: string): string {
  if (!next) {
    return existing ?? "";
  }
  if (!existing) {
    return next;
  }
  return `${existing}\n\n${next}`;
}

function normalizeSessionIdFromPath(rolloutPath: string): string | undefined {
  const match = rolloutPath.match(
    /([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\.jsonl$/i,
  );
  return match?.[1];
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

class SessionProjectionBuilder {
  private sessionId?: string;
  private createdAt?: Date;
  private updatedAt?: Date;
  private cwd = "";
  private projectRoot?: string;
  private repoRoot?: string;
  private gitBranch?: string;
  private originator?: string;
  private source?: string;
  private cliVersion?: string;
  private modelProvider?: string;
  private modelName?: string;
  private reasoningEffort?: string;
  private agentNickname?: string;
  private agentRole?: string;
  private agentPath?: string;
  private firstUserMessage?: string;
  private latestUserMessage?: string;
  private lastUserAt?: Date;
  private lastAssistantAt?: Date;
  private lastCompletionAt?: Date;
  private turns: MutableTurn[] = [];
  private turnById = new Map<string, MutableTurn>();
  private currentTurnId?: string;
  private lastTurnContext?: ProjectionJsonValue;

  constructor(private readonly rolloutPath: string) {}

  handleLine(line: RolloutLine, lineTimestamp: Date): void {
    this.updatedAt = lineTimestamp;
    const payload = isRecord(line.payload) ? line.payload : undefined;
    switch (line.type) {
      case "session_meta":
        if (payload) {
          this.handleSessionMeta(payload, lineTimestamp);
        }
        break;
      case "turn_context":
        if (payload) {
          this.handleTurnContext(payload);
        }
        break;
      case "event_msg":
        if (payload) {
          this.handleEvent(payload, lineTimestamp);
        }
        break;
      default:
        break;
    }
  }

  finish(lineCount: number): BuiltSessionProjection | null {
    const sessionId = this.sessionId ?? normalizeSessionIdFromPath(this.rolloutPath);
    if (!sessionId || !this.createdAt || !this.updatedAt || !this.cwd) {
      return null;
    }

    const turns = this.turns.map((turn): CodexTurnProjection => ({
      turnId: turn.turnId,
      sequence: turn.sequence,
      status: turn.status,
      userMessage: turn.userMessage,
      assistantMessage: this.joinAssistantSegments(turn),
      assistantPartial: turn.assistantPartial,
      planText: turn.planText,
      reasoningSummary: turn.reasoningSummary,
      startedAt: turn.startedAt,
      completedAt: turn.completedAt,
      durationMs: turn.durationMs,
      updatedAt: turn.updatedAt,
    }));
    const lastTurn = turns.at(-1);
    const latestAssistantMessage = [...turns]
      .reverse()
      .map((turn) => turn.assistantMessage)
      .find((message) => !!message);
    const firstUserMessage =
      turns.find((turn) => !!turn.userMessage)?.userMessage ?? this.firstUserMessage;
    const latestUserMessage =
      [...turns].reverse().map((turn) => turn.userMessage).find((message) => !!message) ??
      this.latestUserMessage;
    const latestAssistantPartial =
      lastTurn?.assistantPartial ?? lastTurn?.planText ?? undefined;
    const latestPreview =
      latestAssistantPartial ??
      latestAssistantMessage ??
      latestUserMessage ??
      firstUserMessage;
    const lastUserAt =
      latestTurnDate(
        turns,
        (turn) => !!turn.userMessage,
        (turn) => {
          const mutableTurn = this.turnById.get(turn.turnId);
          return mutableTurn?.lastUserAt ?? turn.updatedAt ?? turn.startedAt;
        },
      ) ?? this.lastUserAt;
    const lastAssistantAt =
      latestTurnDate(
        turns,
        (turn) => !!turn.assistantMessage || !!turn.assistantPartial,
        (turn) => {
          const mutableTurn = this.turnById.get(turn.turnId);
          return mutableTurn?.lastAssistantAt ?? turn.updatedAt ?? turn.completedAt ?? turn.startedAt;
        },
      ) ?? this.lastAssistantAt;
    const lastCompletionAt =
      latestTurnDate(turns, (turn) => !!turn.completedAt, (turn) => turn.completedAt) ??
      this.lastCompletionAt;

    const projection: CodexSessionProjection = {
      sessionId,
      rolloutPath: this.rolloutPath,
      cwd: this.cwd,
      projectRoot: this.projectRoot ?? this.cwd,
      repoRoot: this.repoRoot,
      gitBranch: this.gitBranch,
      originator: this.originator,
      source: this.source,
      cliVersion: this.cliVersion,
      modelProvider: this.modelProvider,
      modelName: this.modelName,
      reasoningEffort: this.reasoningEffort,
      agentNickname: this.agentNickname,
      agentRole: this.agentRole,
      agentPath: this.agentPath,
      firstUserMessage,
      latestUserMessage,
      latestAssistantMessage,
      latestAssistantPartial,
      latestPreview,
      status: lastTurn?.status ?? "idle",
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      latestActivityAt: this.updatedAt,
      lastUserAt,
      lastAssistantAt,
      lastCompletionAt,
      metadataJson: this.lastTurnContext
        ? { last_turn_context: this.lastTurnContext }
        : undefined,
      turns,
    };

    return { projection, lineCount };
  }

  private handleSessionMeta(payload: RolloutPayload, lineTimestamp: Date): void {
    this.sessionId = getString(payload.id) ?? this.sessionId;
    this.cwd = getString(payload.cwd) ?? this.cwd;
    this.projectRoot = this.cwd || this.projectRoot;
    const git = isRecord(payload.git) ? payload.git : undefined;
    this.gitBranch = getString(git?.branch) ?? this.gitBranch;
    this.originator = getString(payload.originator) ?? this.originator;
    this.source = getString(payload.source) ?? this.source;
    this.cliVersion = getString(payload.cli_version) ?? this.cliVersion;
    this.modelProvider = getString(payload.model_provider) ?? this.modelProvider;
    this.agentNickname = getString(payload.agent_nickname) ?? this.agentNickname;
    this.agentRole = getString(payload.agent_role) ?? this.agentRole;
    this.agentPath = getString(payload.agent_path) ?? this.agentPath;
    this.createdAt = parseIsoDate(payload.timestamp) ?? this.createdAt ?? lineTimestamp;
  }

  private handleTurnContext(payload: RolloutPayload): void {
    this.lastTurnContext = payload as ProjectionJsonValue;
    const turnId = getString(payload.turn_id);
    if (turnId) {
      this.ensureTurn(turnId, this.updatedAt ?? this.createdAt ?? new Date());
    }
    const cwd = getString(payload.cwd);
    if (cwd) {
      this.cwd = cwd;
      this.projectRoot = cwd;
    }
    const repoRoot =
      getString(payload.repo_root) ??
      getString(payload.repoRoot);
    if (repoRoot) {
      this.repoRoot = repoRoot;
    }
    this.modelName = getString(payload.model) ?? this.modelName;
    this.reasoningEffort =
      getString(payload.effort) ??
      getString(payload.reasoning_effort) ??
      this.reasoningEffort;
  }

  private handleEvent(payload: RolloutPayload, lineTimestamp: Date): void {
    const eventType = getString(payload.type);
    if (!eventType) {
      return;
    }

    switch (eventType) {
      case "task_started":
      case "turn_started":
        this.handleTurnStarted(payload, lineTimestamp);
        break;
      case "task_complete":
      case "turn_complete":
        this.handleTurnComplete(payload, lineTimestamp);
        break;
      case "turn_aborted":
        this.handleTurnAborted(payload, lineTimestamp);
        break;
      case "user_message":
        this.handleUserMessage(payload, lineTimestamp);
        break;
      case "agent_message":
        this.handleAgentMessage(payload, lineTimestamp);
        break;
      case "agent_message_content_delta":
        this.handleAgentMessageDelta(payload, lineTimestamp);
        break;
      case "plan_delta":
        this.handlePlanDelta(payload, lineTimestamp);
        break;
      case "reasoning_content_delta":
        this.handleReasoningSummaryDelta(payload, lineTimestamp);
        break;
      case "thread_rolled_back":
        this.handleThreadRolledBack(payload);
        break;
      case "error":
      case "stream_error":
        this.handleTurnFailure(lineTimestamp);
        break;
      default:
        break;
    }
  }

  private handleTurnStarted(payload: RolloutPayload, lineTimestamp: Date): void {
    const turnId = getString(payload.turn_id);
    const turn = this.ensureTurn(turnId, lineTimestamp);
    turn.status = "in_progress";
    turn.startedAt = parseUnixSeconds(payload.started_at) ?? turn.startedAt ?? lineTimestamp;
    turn.updatedAt = lineTimestamp;
    this.currentTurnId = turn.turnId;
  }

  private handleTurnComplete(payload: RolloutPayload, lineTimestamp: Date): void {
    const turn = this.ensureTurn(getString(payload.turn_id), lineTimestamp);
    const lastAgentMessage = getString(payload.last_agent_message);
    if (lastAgentMessage) {
      this.appendAssistantSegment(turn, lastAgentMessage);
      turn.assistantPartial = undefined;
    }
    turn.status = "completed";
    turn.completedAt = parseUnixSeconds(payload.completed_at) ?? lineTimestamp;
    turn.durationMs = getNumber(payload.duration_ms) ?? turn.durationMs;
    turn.updatedAt = lineTimestamp;
    this.lastCompletionAt = turn.completedAt;
    if (this.currentTurnId === turn.turnId) {
      this.currentTurnId = undefined;
    }
  }

  private handleTurnAborted(payload: RolloutPayload, lineTimestamp: Date): void {
    const turn = this.ensureTurn(getString(payload.turn_id), lineTimestamp);
    turn.status = "interrupted";
    turn.completedAt = parseUnixSeconds(payload.completed_at) ?? lineTimestamp;
    turn.durationMs = getNumber(payload.duration_ms) ?? turn.durationMs;
    turn.updatedAt = lineTimestamp;
    if (this.currentTurnId === turn.turnId) {
      this.currentTurnId = undefined;
    }
  }

  private handleUserMessage(payload: RolloutPayload, lineTimestamp: Date): void {
    const message = getString(payload.message);
    if (!message) {
      return;
    }
    const turn = this.ensureTurn(undefined, lineTimestamp);
    turn.userMessage = appendText(turn.userMessage, message);
    turn.lastUserAt = lineTimestamp;
    turn.updatedAt = lineTimestamp;
    this.firstUserMessage ??= message;
    this.latestUserMessage = turn.userMessage;
    this.lastUserAt = lineTimestamp;
  }

  private handleAgentMessage(payload: RolloutPayload, lineTimestamp: Date): void {
    const message = getString(payload.message);
    if (!message) {
      return;
    }
    const turn = this.ensureTurn(undefined, lineTimestamp);
    this.appendAssistantSegment(turn, message);
    turn.assistantPartial = undefined;
    turn.lastAssistantAt = lineTimestamp;
    turn.updatedAt = lineTimestamp;
    this.lastAssistantAt = lineTimestamp;
  }

  private handleAgentMessageDelta(payload: RolloutPayload, lineTimestamp: Date): void {
    const delta = getString(payload.delta);
    if (!delta) {
      return;
    }
    const turn = this.ensureTurn(getString(payload.turn_id), lineTimestamp);
    turn.assistantPartial = `${turn.assistantPartial ?? ""}${delta}`;
    turn.lastAssistantAt = lineTimestamp;
    turn.updatedAt = lineTimestamp;
    this.lastAssistantAt = lineTimestamp;
  }

  private handlePlanDelta(payload: RolloutPayload, lineTimestamp: Date): void {
    const delta = getString(payload.delta);
    if (!delta) {
      return;
    }
    const turn = this.ensureTurn(getString(payload.turn_id), lineTimestamp);
    turn.planText = `${turn.planText ?? ""}${delta}`;
    turn.updatedAt = lineTimestamp;
  }

  private handleReasoningSummaryDelta(payload: RolloutPayload, lineTimestamp: Date): void {
    const delta = getString(payload.delta);
    if (!delta) {
      return;
    }
    const turn = this.ensureTurn(getString(payload.turn_id), lineTimestamp);
    turn.reasoningSummary = `${turn.reasoningSummary ?? ""}${delta}`;
    turn.updatedAt = lineTimestamp;
  }

  private handleThreadRolledBack(payload: RolloutPayload): void {
    const numTurns = getNumber(payload.num_turns) ?? 0;
    if (numTurns <= 0) {
      return;
    }
    const remaining = Math.max(0, this.turns.length - numTurns);
    this.turns = this.turns.slice(0, remaining);
    this.turnById = new Map(this.turns.map((turn) => [turn.turnId, turn]));
    if (this.currentTurnId && !this.turnById.has(this.currentTurnId)) {
      this.currentTurnId = undefined;
    }
  }

  private handleTurnFailure(lineTimestamp: Date): void {
    const currentTurn = this.currentTurnId ? this.turnById.get(this.currentTurnId) : undefined;
    if (!currentTurn) {
      return;
    }
    currentTurn.status = "failed";
    currentTurn.updatedAt = lineTimestamp;
  }

  private ensureTurn(turnId: string | undefined, lineTimestamp: Date): MutableTurn {
    if (!turnId && this.currentTurnId) {
      const currentTurn = this.turnById.get(this.currentTurnId);
      if (currentTurn) {
        return currentTurn;
      }
    }

    const resolvedTurnId = turnId ?? `synthetic-${this.turns.length + 1}`;
    const existing = this.turnById.get(resolvedTurnId);
    if (existing) {
      this.currentTurnId = existing.turnId;
      return existing;
    }

    const created: MutableTurn = {
      turnId: resolvedTurnId,
      sequence: this.turns.length + 1,
      status: "pending",
      assistantSegments: [],
      updatedAt: lineTimestamp,
    };
    this.turns.push(created);
    this.turnById.set(created.turnId, created);
    this.currentTurnId = created.turnId;
    return created;
  }

  private appendAssistantSegment(turn: MutableTurn, message: string): void {
    if (!message) {
      return;
    }
    const previous = turn.assistantSegments.at(-1);
    if (previous === message) {
      return;
    }
    turn.assistantSegments.push(message);
  }

  private joinAssistantSegments(turn: MutableTurn): string | undefined {
    if (turn.assistantSegments.length === 0) {
      return undefined;
    }
    return turn.assistantSegments.join("\n\n");
  }
}

async function collectRolloutPaths(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const paths: string[] = [];

  for (const entry of entries) {
    const absolutePath = join(root, entry.name);
    if (entry.isDirectory()) {
      paths.push(...(await collectRolloutPaths(absolutePath)));
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      paths.push(absolutePath);
    }
  }

  return paths.sort();
}

async function collectNewestRolloutPaths(root: string, limit: number): Promise<string[]> {
  const paths: string[] = [];
  await collectNewestRolloutPathsInto(root, limit, paths);
  return paths;
}

async function collectNewestRolloutPathsInto(
  root: string,
  limit: number,
  paths: string[],
): Promise<void> {
  if (paths.length >= limit) {
    return;
  }

  const entries = (await readdir(root, { withFileTypes: true }))
    .slice()
    .sort((left, right) => right.name.localeCompare(left.name));

  for (const entry of entries) {
    if (paths.length >= limit) {
      return;
    }

    const absolutePath = join(root, entry.name);
    if (entry.isDirectory()) {
      await collectNewestRolloutPathsInto(absolutePath, limit, paths);
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      paths.push(absolutePath);
    }
  }
}

async function findRolloutPathForSession(
  root: string,
  sessionId: string,
): Promise<{ rolloutPath: string | null; scanned: number }> {
  const entries = await readdir(root, { withFileTypes: true });
  const expectedSuffix = `-${sessionId}.jsonl`;
  let scanned = 0;

  for (const entry of entries) {
    const absolutePath = join(root, entry.name);
    if (entry.isDirectory()) {
      const nested = await findRolloutPathForSession(absolutePath, sessionId);
      scanned += nested.scanned;
      if (nested.rolloutPath) {
        return { rolloutPath: nested.rolloutPath, scanned };
      }
      continue;
    }
    if (entry.isFile() && entry.name.startsWith("rollout-") && entry.name.endsWith(".jsonl")) {
      scanned += 1;
      if (entry.name.endsWith(expectedSuffix)) {
        return { rolloutPath: absolutePath, scanned };
      }
    }
  }

  return { rolloutPath: null, scanned };
}

async function syncRolloutPath(
  store: CodexSessionStore,
  rolloutPath: string,
  onProjectionSynced: ((event: SyncedProjectionEvent) => void) | undefined,
  signal?: AbortSignal,
): Promise<boolean> {
  if (signal?.aborted) {
    return false;
  }

  const [fileStat, syncState] = await Promise.all([
    stat(rolloutPath),
    store.getSyncStateByPath(rolloutPath),
  ]);
  if (syncState && syncState.synced_at.getTime() >= fileStat.mtime.getTime()) {
    return false;
  }

  const rolloutText = await readFile(rolloutPath, "utf8");
  const built = buildSessionProjectionFromRollout(rolloutPath, rolloutText);
  if (!built) {
    return false;
  }

  await store.replaceSessionProjection(built.projection, {
    sourceId: rolloutPath,
    absolutePath: rolloutPath,
    sessionId: built.projection.sessionId,
    lineCount: built.lineCount,
    syncedAt: fileStat.mtime,
  });
  onProjectionSynced?.({
    rolloutPath,
    syncedAt: fileStat.mtime,
    lineCount: built.lineCount,
    projection: built.projection,
    completionEvents: completionEventsFromProjection(built.projection),
  });
  return true;
}

async function syncRolloutPaths(
  store: CodexSessionStore,
  rolloutPaths: string[],
  onProjectionSynced: ((event: SyncedProjectionEvent) => void) | undefined,
  signal?: AbortSignal,
): Promise<{ scanned: number; synced: number }> {
  let synced = 0;

  for (const rolloutPath of rolloutPaths) {
    if (signal?.aborted) {
      break;
    }
    if (await syncRolloutPath(store, rolloutPath, onProjectionSynced, signal)) {
      synced += 1;
    }
  }

  return { scanned: rolloutPaths.length, synced };
}

export function completionEventsFromProjection(
  projection: CodexSessionProjection,
): CodexCompletionEvent[] {
  const projectPath = projection.projectRoot ?? projection.cwd;
  const projectName = basename(projectPath);
  const source = projection.source ?? projection.originator ?? "codex";

  return projection.turns.flatMap((turn) => {
    if (turn.status !== "completed" || !turn.completedAt) {
      return [];
    }

    const completedAt = coerceDate(turn.completedAt);
    const updatedAt = coerceDate(turn.updatedAt ?? turn.completedAt);
    return [{
      id: `${projection.sessionId}-${turn.turnId}`,
      sessionId: projection.sessionId,
      turnId: turn.turnId,
      projectPath,
      projectName,
      source,
      summary: turn.assistantMessage ?? turn.assistantPartial ?? undefined,
      status: turn.status,
      timestamp: completedAt,
      completedAt,
      updatedAt,
    }];
  });
}

export async function syncRecentSessionsForProjectRoot(
  options: SyncRecentProjectSessionsOptions,
): Promise<{ scanned: number; matched: number; synced: number }> {
  if (options.limit <= 0) {
    return { scanned: 0, matched: 0, synced: 0 };
  }

  const sessionsRoot = join(options.codexHome, "sessions");
  const rolloutPaths = (await collectRolloutPaths(sessionsRoot)).reverse();
  let scanned = 0;
  let matched = 0;
  let synced = 0;

  for (const rolloutPath of rolloutPaths) {
    if (options.signal?.aborted) {
      break;
    }

    scanned += 1;

    const rolloutText = await readFile(rolloutPath, "utf8");
    const built = buildSessionProjectionFromRollout(rolloutPath, rolloutText);
    if (!built || built.projection.projectRoot !== options.projectRoot) {
      continue;
    }

    matched += 1;
    const didSync = await syncRolloutPath(
      options.store,
      rolloutPath,
      options.onProjectionSynced,
      options.signal,
    );
    if (didSync) {
      synced += 1;
    }

    if (matched >= options.limit) {
      break;
    }
  }

  return { scanned, matched, synced };
}

export async function syncSessionsByPrefix(
  options: SyncSessionPrefixOptions,
): Promise<{ scanned: number; matched: number; synced: number }> {
  if (options.limit <= 0) {
    return { scanned: 0, matched: 0, synced: 0 };
  }

  const prefix = options.prefix.trim().toLowerCase();
  if (!prefix) {
    return { scanned: 0, matched: 0, synced: 0 };
  }

  const sessionsRoot = join(options.codexHome, "sessions");
  const rolloutPaths = (await collectRolloutPaths(sessionsRoot)).reverse();
  let scanned = 0;
  let matched = 0;
  let synced = 0;

  for (const rolloutPath of rolloutPaths) {
    if (options.signal?.aborted) {
      break;
    }

    scanned += 1;
    const sessionId = normalizeSessionIdFromPath(rolloutPath)?.toLowerCase();
    if (!sessionId || !sessionId.startsWith(prefix)) {
      continue;
    }

    matched += 1;
    const didSync = await syncRolloutPath(
      options.store,
      rolloutPath,
      options.onProjectionSynced,
      options.signal,
    );
    if (didSync) {
      synced += 1;
    }

    if (matched >= options.limit) {
      break;
    }
  }

  return { scanned, matched, synced };
}

export function buildSessionProjectionFromRollout(
  rolloutPath: string,
  rolloutText: string,
): BuiltSessionProjection | null {
  const builder = new SessionProjectionBuilder(rolloutPath);
  let lineCount = 0;

  for (const rawLine of rolloutText.split("\n")) {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      continue;
    }
    lineCount += 1;

    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      continue;
    }
    if (!isRecord(parsed) || typeof parsed.type !== "string") {
      continue;
    }
    const line: RolloutLine = {
      timestamp: getString(parsed.timestamp),
      type: parsed.type,
      payload: isRecord(parsed.payload) ? parsed.payload : undefined,
    };
    const lineTimestamp = parseIsoDate(line.timestamp) ?? new Date();
    builder.handleLine(line, lineTimestamp);
  }

  return builder.finish(lineCount);
}

export async function syncCodexRollouts(
  options: SyncCodexRolloutsOptions,
): Promise<{ scanned: number; synced: number }> {
  const sessionsRoot = join(options.codexHome, "sessions");
  const rolloutPaths = (await collectRolloutPaths(sessionsRoot)).reverse();
  return syncRolloutPaths(
    options.store,
    rolloutPaths,
    options.onProjectionSynced,
    options.signal,
  );
}

export async function syncNewestCodexRollouts(
  options: SyncNewestCodexRolloutsOptions,
): Promise<{ scanned: number; synced: number }> {
  const sessionsRoot = join(options.codexHome, "sessions");
  const rolloutPaths = await collectNewestRolloutPaths(
    sessionsRoot,
    Math.max(1, options.limit),
  );
  return syncRolloutPaths(
    options.store,
    rolloutPaths,
    options.onProjectionSynced,
    options.signal,
  );
}

export async function syncCodexSessionRollout(
  options: SyncCodexSessionRolloutOptions,
): Promise<{ scanned: number; synced: number; found: boolean }> {
  const existingSession = await options.store.getSession(options.sessionId);
  if (existingSession) {
    const synced = await syncRolloutPath(
      options.store,
      existingSession.rollout_path,
      options.onProjectionSynced,
      options.signal,
    );
    const backfilled = synced
      ? false
      : await options.store.ensureNativeCodexSessionRun(options.sessionId);
    return {
      scanned: 1,
      synced: synced || backfilled ? 1 : 0,
      found: true,
    };
  }

  const sessionsRoot = join(options.codexHome, "sessions");
  const located = await findRolloutPathForSession(sessionsRoot, options.sessionId);
  if (!located.rolloutPath) {
    return { scanned: located.scanned, synced: 0, found: false };
  }

  const synced = await syncRolloutPath(
    options.store,
    located.rolloutPath,
    options.onProjectionSynced,
    options.signal,
  );
  return {
    scanned: located.scanned,
    synced: synced ? 1 : 0,
    found: true,
  };
}

export async function watchCodexRollouts(
  options: WatchCodexRolloutsOptions,
): Promise<void> {
  const pollIntervalMs = options.pollIntervalMs ?? 1000;
  const recentScanLimit = Math.max(1, options.recentScanLimit ?? 128);
  const fullRescanEveryMs = Math.max(pollIntervalMs, options.fullRescanEveryMs ?? 60_000);
  const sessionsRoot = join(options.codexHome, "sessions");
  let lastFullScanAt = Date.now();

  while (!options.signal?.aborted) {
    const now = Date.now();
    const result =
      now - lastFullScanAt >= fullRescanEveryMs
        ? await syncCodexRollouts(options)
        : await syncRolloutPaths(
            options.store,
            await collectNewestRolloutPaths(sessionsRoot, recentScanLimit),
            options.onProjectionSynced,
            options.signal,
          );
    if (now - lastFullScanAt >= fullRescanEveryMs) {
      lastFullScanAt = now;
    }
    options.onCycle?.(result);
    await delay(pollIntervalMs, options.signal);
  }
}

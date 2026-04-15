import { randomUUID } from "node:crypto";
import { basename } from "node:path";
import {
  createJazzContext,
  type Db,
  type JazzContext,
  type Session,
  type TableProxy,
} from "jazz-tools/backend";
import {
  app,
  type CodexSession,
  type CodexSessionInit,
  type CodexSessionPresence,
  type CodexSessionPresenceInit,
  type CodexSyncState,
  type CodexSyncStateInit,
  type CodexTurn,
  type CodexTurnInit,
  type JAgentArtifact,
  type JAgentArtifactInit,
  type JAgentAttempt,
  type JAgentAttemptInit,
  type JAgentDefinition,
  type JAgentDefinitionInit,
  type JAgentRun,
  type JAgentRunInit,
  type JAgentSessionBinding,
  type JAgentSessionBindingInit,
  type JAgentStep,
  type JAgentStepInit,
  type JAgentWait,
  type JAgentWaitInit,
  type JsonValue,
} from "../schema/app.js";

type DurabilityTier = "worker" | "edge" | "global";
type TimestampInput = Date | string | number;

const DEFAULT_APP_ID = "codex-sessions";
const TERMINAL_AGENT_RUN_STATUSES = new Set([
  "completed",
  "failed",
  "cancelled",
  "error",
]);
const NATIVE_CODEX_SESSION_DEFINITION_ID = "native:codex-session";
const NATIVE_CODEX_SESSION_DEFINITION_NAME = "native-codex-session";
const NATIVE_CODEX_SESSION_DEFINITION_VERSION = "v1";
const NATIVE_CODEX_SESSION_SOURCE_KIND = "native_codex_session";
const NATIVE_CODEX_SESSION_ENTRYPOINT = "codex-rollout";
const NATIVE_CODEX_SESSION_TRIGGER_SOURCE = "native-codex-session";
const NATIVE_CODEX_SESSION_BINDING_ROLE = "primary_session";
const ACTIVE_CODEX_PRESENCE_STATES = ["starting", "running", "streaming", "waiting"] as const;
const ACTIVE_CODEX_PRESENCE_STATE_SET = new Set<string>(ACTIVE_CODEX_PRESENCE_STATES);
const DEFAULT_ACTIVE_CODEX_PRESENCE_MAX_AGE_MS = 10 * 365 * 24 * 60 * 60 * 1000;
const TERMINAL_CODEX_TURN_STATUSES = new Set(["completed", "failed", "interrupted"]);

export interface CodexSessionStoreConfig {
  dataPath: string;
  appId?: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  serverPathPrefix?: string;
  backendSecret?: string;
  adminSecret?: string;
  tier?: DurabilityTier;
}

export interface CodexTurnProjection {
  turnId: string;
  sequence: number;
  status: string;
  userMessage?: string;
  assistantMessage?: string;
  assistantPartial?: string;
  planText?: string;
  reasoningSummary?: string;
  startedAt?: TimestampInput;
  completedAt?: TimestampInput;
  durationMs?: number;
  updatedAt?: TimestampInput;
}

export interface CodexSessionProjection {
  sessionId: string;
  rolloutPath: string;
  cwd: string;
  projectRoot?: string;
  repoRoot?: string;
  gitBranch?: string;
  originator?: string;
  source?: string;
  cliVersion?: string;
  modelProvider?: string;
  modelName?: string;
  reasoningEffort?: string;
  agentNickname?: string;
  agentRole?: string;
  agentPath?: string;
  firstUserMessage?: string;
  latestUserMessage?: string;
  latestAssistantMessage?: string;
  latestAssistantPartial?: string;
  latestPreview?: string;
  status: string;
  createdAt: TimestampInput;
  updatedAt: TimestampInput;
  latestActivityAt?: TimestampInput;
  lastUserAt?: TimestampInput;
  lastAssistantAt?: TimestampInput;
  lastCompletionAt?: TimestampInput;
  metadataJson?: JsonValue;
  turns: CodexTurnProjection[];
}

export interface CodexSyncStateInput {
  sourceId: string;
  absolutePath: string;
  sessionId?: string;
  lineCount: number;
  syncedAt?: TimestampInput;
}

export interface CodexCompletionEvent {
  id: string;
  sessionId: string;
  turnId: string;
  projectPath: string;
  projectName: string;
  source: string;
  summary?: string;
  status: string;
  timestamp: Date;
  completedAt: Date;
  updatedAt: Date;
}

export interface CodexSessionSummary {
  session: CodexSession;
  turns: CodexTurn[];
  syncState: CodexSyncState | null;
  presence: CodexSessionPresence | null;
}

export interface CodexSessionPresenceSummary {
  presence: CodexSessionPresence;
  session: CodexSession;
  currentTurn: CodexTurn | null;
}

export interface UpsertJAgentDefinitionInput {
  definitionId: string;
  name: string;
  version: string;
  sourceKind: string;
  entrypoint: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordJAgentRunStartedInput {
  runId: string;
  definitionId: string;
  status?: string;
  projectRoot: string;
  repoRoot?: string;
  cwd?: string;
  triggerSource?: string;
  parentSessionId?: string;
  parentTurnId?: string;
  initiatorSessionId?: string;
  requestedRole?: string;
  requestedModel?: string;
  requestedReasoningEffort?: string;
  forkTurns?: number;
  currentStepKey?: string;
  inputJson?: JsonValue;
  startedAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordJAgentRunCompletedInput {
  runId: string;
  status?: string;
  outputJson?: JsonValue;
  errorText?: string;
  currentStepKey?: string;
  completedAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordJAgentStepStartedInput {
  runId: string;
  stepId: string;
  sequence: number;
  stepKey: string;
  stepKind: string;
  status?: string;
  inputJson?: JsonValue;
  startedAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordJAgentStepCompletedInput {
  runId: string;
  stepId: string;
  status?: string;
  outputJson?: JsonValue;
  errorText?: string;
  completedAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordJAgentAttemptStartedInput {
  runId: string;
  stepId: string;
  attempt: number;
  attemptId?: string;
  status?: string;
  codexSessionId?: string;
  codexTurnId?: string;
  forkTurns?: number;
  modelName?: string;
  reasoningEffort?: string;
  startedAt?: TimestampInput;
}

export interface RecordJAgentAttemptCompletedInput {
  runId: string;
  stepId: string;
  attemptId: string;
  status?: string;
  completedAt?: TimestampInput;
  errorText?: string;
}

export interface RecordJAgentWaitStartedInput {
  runId: string;
  stepId: string;
  waitKind: string;
  waitId?: string;
  targetSessionId?: string;
  targetTurnId?: string;
  resumeConditionJson?: JsonValue;
  status?: string;
  startedAt?: TimestampInput;
}

export interface RecordJAgentWaitResolvedInput {
  runId: string;
  waitId: string;
  status?: string;
  resumedAt?: TimestampInput;
}

export interface BindJAgentSessionInput {
  runId: string;
  codexSessionId: string;
  bindingRole: string;
  bindingId?: string;
  parentSessionId?: string;
  createdAt?: TimestampInput;
}

export interface RecordJAgentArtifactInput {
  runId: string;
  kind: string;
  path: string;
  artifactId?: string;
  stepId?: string;
  textPreview?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
}

export interface JAgentRunSummary {
  definition: JAgentDefinition;
  run: JAgentRun;
  steps: JAgentStep[];
  attempts: JAgentAttempt[];
  waits: JAgentWait[];
  sessionBindings: JAgentSessionBinding[];
  artifacts: JAgentArtifact[];
  boundSessions: CodexSession[];
}

function asDate(value?: TimestampInput): Date {
  if (value instanceof Date) return value;
  if (typeof value === "string" || typeof value === "number") {
    return new Date(value);
  }
  return new Date();
}

function pruneUndefined<T extends Record<string, unknown>>(input: T): Partial<T> {
  const entries = Object.entries(input).filter(([, value]) => value !== undefined);
  return Object.fromEntries(entries) as Partial<T>;
}

function clampLimit(limit: number | undefined, fallback = 20): number {
  return Math.max(1, Math.min(limit ?? fallback, 1000));
}

function nullable<T>(value: T | undefined): T | null {
  return value ?? null;
}

function sessionPreview(projection: CodexSessionProjection): string | undefined {
  return (
    projection.latestPreview ??
    projection.latestAssistantPartial ??
    projection.latestAssistantMessage ??
    projection.latestUserMessage ??
    projection.firstUserMessage
  );
}

function latestProjectionDate(values: Array<TimestampInput | undefined>): Date | undefined {
  const timestamps = values
    .filter((value): value is TimestampInput => value !== undefined)
    .map((value) => asDate(value).getTime())
    .filter((value) => !Number.isNaN(value));
  if (timestamps.length === 0) {
    return undefined;
  }
  return new Date(Math.max(...timestamps));
}

function uniqueStrings(values: Iterable<string>): string[] {
  return [...new Set(values)];
}

function dedupeRowsByKey<T>(
  rows: T[],
  keyForRow: (row: T) => string,
): T[] {
  const seen = new Set<string>();
  const deduped: T[] = [];

  for (const row of rows) {
    const key = keyForRow(row);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(row);
  }

  return deduped;
}

function normalizeRow<T extends Record<string, unknown>>(row: T): T {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, value === null ? undefined : value]),
  ) as T;
}

function nativeCodexSessionRunId(sessionId: string): string {
  return `native-session:${sessionId}`;
}

function nativeCodexSessionRunStatus(status: string): string {
  switch (status) {
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "interrupted":
      return "cancelled";
    default:
      return "running";
  }
}

function normalizeStatus(value: string | undefined): string {
  return value?.trim().toLowerCase() ?? "";
}

function activeProjectionTurn(
  turns: CodexTurnProjection[],
): CodexTurnProjection | undefined {
  return [...turns]
    .reverse()
    .find((turn) => !TERMINAL_CODEX_TURN_STATUSES.has(normalizeStatus(turn.status)));
}

function derivePresenceState(projection: CodexSessionProjection): string {
  const currentTurn = activeProjectionTurn(projection.turns);
  const sessionStatus = normalizeStatus(projection.status);

  if (currentTurn) {
    const turnStatus = normalizeStatus(currentTurn.status);
    if (turnStatus === "pending") {
      return "starting";
    }
    if (
      currentTurn.assistantPartial?.trim() ||
      currentTurn.planText?.trim() ||
      currentTurn.reasoningSummary?.trim() ||
      projection.latestAssistantPartial?.trim()
    ) {
      return "streaming";
    }
    return "running";
  }

  switch (sessionStatus) {
    case "pending":
      return "starting";
    case "in_progress":
      return projection.latestAssistantPartial?.trim() ? "streaming" : "running";
    case "waiting":
      return "waiting";
    case "completed":
      return "completed";
    case "failed":
    case "error":
      return "failed";
    case "interrupted":
    case "aborted":
    case "cancelled":
      return "interrupted";
    default:
      return "idle";
  }
}

function activePresenceMaxAgeMs(): number {
  const raw = Number(process.env.CODEX_ACTIVE_SESSION_MAX_AGE_MS ?? DEFAULT_ACTIVE_CODEX_PRESENCE_MAX_AGE_MS);
  if (!Number.isFinite(raw) || raw <= 0) {
    return DEFAULT_ACTIVE_CODEX_PRESENCE_MAX_AGE_MS;
  }
  return raw;
}

function latestPresenceRefreshAt(row: CodexSessionPresence): Date {
  const candidates = [
    row.last_heartbeat_at,
    row.last_event_at,
    row.latest_activity_at,
    row.updated_at,
  ].filter((value): value is Date => value instanceof Date);
  return new Date(Math.max(...candidates.map((value) => value.getTime())));
}

function isFreshActivePresence(row: CodexSessionPresence, now = Date.now()): boolean {
  return now - latestPresenceRefreshAt(row).getTime() <= activePresenceMaxAgeMs();
}

function presenceLastEventAt(projection: CodexSessionProjection): Date {
  return (
    latestProjectionDate([
      projection.latestActivityAt,
      projection.lastAssistantAt,
      projection.lastUserAt,
      projection.lastCompletionAt,
      activeProjectionTurn(projection.turns)?.updatedAt,
      projection.updatedAt,
    ]) ?? asDate(projection.updatedAt)
  );
}

function projectionFromStoredSession(
  codexSession: CodexSession,
  turns: CodexTurn[],
): CodexSessionProjection {
  return {
    sessionId: codexSession.session_id,
    rolloutPath: codexSession.rollout_path,
    cwd: codexSession.cwd,
    projectRoot: codexSession.project_root,
    repoRoot: codexSession.repo_root,
    gitBranch: codexSession.git_branch,
    originator: codexSession.originator,
    source: codexSession.source,
    cliVersion: codexSession.cli_version,
    modelProvider: codexSession.model_provider,
    modelName: codexSession.model_name,
    reasoningEffort: codexSession.reasoning_effort,
    agentNickname: codexSession.agent_nickname,
    agentRole: codexSession.agent_role,
    agentPath: codexSession.agent_path,
    firstUserMessage: codexSession.first_user_message,
    latestUserMessage: codexSession.latest_user_message,
    latestAssistantMessage: codexSession.latest_assistant_message,
    latestAssistantPartial: codexSession.latest_assistant_partial,
    latestPreview: codexSession.latest_preview,
    status: codexSession.status,
    createdAt: codexSession.created_at,
    updatedAt: codexSession.updated_at,
    latestActivityAt: codexSession.latest_activity_at,
    lastUserAt: codexSession.last_user_at,
    lastAssistantAt: codexSession.last_assistant_at,
    lastCompletionAt: codexSession.last_completion_at,
    metadataJson: codexSession.metadata_json,
    turns: turns.map((turn) => ({
      turnId: turn.turn_id,
      sequence: turn.sequence,
      status: turn.status,
      userMessage: turn.user_message,
      assistantMessage: turn.assistant_message,
      assistantPartial: turn.assistant_partial,
      planText: turn.plan_text,
      reasoningSummary: turn.reasoning_summary,
      startedAt: turn.started_at,
      completedAt: turn.completed_at,
      durationMs: turn.duration_ms,
      updatedAt: turn.updated_at,
    })),
  };
}

export class CodexSessionStore {
  private writeQueue: Promise<void> = Promise.resolve();

  constructor(
    private readonly context: JazzContext,
    private readonly writeTier: DurabilityTier,
  ) {}

  flush(): void {
    this.context.flush();
  }

  async shutdown(): Promise<void> {
    await this.context.shutdown();
  }

  async replaceSessionProjection(
    projection: CodexSessionProjection,
    syncState: CodexSyncStateInput,
    session?: Session,
  ): Promise<CodexSessionSummary> {
    return this.withWriteLock(async () => {
      const db = this.getDb(session);
      const codexSession = await this.upsertSession(db, projection);
      await this.upsertTurns(db, codexSession, projection.turns);
      const codexSyncState = await this.upsertSyncState(db, codexSession, syncState);
      await this.upsertSessionPresence(db, codexSession, projection, codexSyncState.synced_at);
      await this.upsertNativeCodexSessionRunFromSession(db, codexSession, session);
      const summary = await this.getSessionSummary(projection.sessionId, session);
      if (!summary) {
        throw new Error(`codex session ${projection.sessionId} not found after upsert`);
      }
      return summary;
    });
  }

  async listSessions(limit?: number, session?: Session): Promise<CodexSession[]> {
    const query = app.codex_sessions.orderBy("latest_activity_at", "desc");
    const rows = await this.getDb(session).all(
      limit === undefined ? query : query.limit(clampLimit(limit)),
    );
    return dedupeRowsByKey(rows.map((row) => normalizeRow(row)), (row) => row.session_id);
  }

  async listSessionsForProjectRoot(
    projectRoot: string,
    limit?: number,
    session?: Session,
  ): Promise<CodexSession[]> {
    const query = app.codex_sessions
      .where({ project_root: projectRoot })
      .orderBy("latest_activity_at", "desc");
    const rows = await this.getDb(session).all(
      limit === undefined ? query : query.limit(clampLimit(limit)),
    );
    return dedupeRowsByKey(rows.map((row) => normalizeRow(row)), (row) => row.session_id);
  }

  async getSessionPresence(
    sessionId: string,
    session?: Session,
  ): Promise<CodexSessionPresence | null> {
    const row = await this.getDb(session).one(app.codex_session_presence.where({ session_id: sessionId }));
    return row ? normalizeRow(row) : null;
  }

  async listSessionPresence(
    options?: {
      projectRoot?: string;
      limit?: number;
      activeOnly?: boolean;
    },
    session?: Session,
  ): Promise<CodexSessionPresence[]> {
    const query = options?.projectRoot
      ? app.codex_session_presence
          .where({ project_root: options.projectRoot })
          .orderBy("last_event_at", "desc")
      : app.codex_session_presence.orderBy("last_event_at", "desc");
    const rows = await this.getDb(session).all(query);
    const filtered = options?.activeOnly
      ? rows.filter((row) => ACTIVE_CODEX_PRESENCE_STATE_SET.has(row.state) && isFreshActivePresence(row))
      : rows;
    const deduped = dedupeRowsByKey(filtered.map((row) => normalizeRow(row)), (row) => row.session_id);
    return options?.limit === undefined
      ? deduped
      : deduped.slice(0, clampLimit(options.limit, 50));
  }

  async listActiveSessionSummaries(
    options?: { projectRoot?: string; limit?: number },
    session?: Session,
  ): Promise<CodexSessionPresenceSummary[]> {
    const db = this.getDb(session);
    const presenceRows = await this.listSessionPresence(
      {
        projectRoot: options?.projectRoot,
        limit: options?.limit,
        activeOnly: true,
      },
      session,
    );

    const summaries = await Promise.all(
      presenceRows.map(async (presence) => {
        const [codexSession, currentTurn] = await Promise.all([
          this.getCodexSessionByExternalId(db, presence.session_id),
          presence.current_turn_id
            ? this.getCodexTurnByExternalId(db, presence.session_id, presence.current_turn_id)
            : Promise.resolve(null),
        ]);
        if (!codexSession) {
          return null;
        }
        return {
          presence,
          session: codexSession,
          currentTurn,
        };
      }),
    );

    return summaries.filter(
      (summary): summary is CodexSessionPresenceSummary => summary !== null,
    );
  }

  async backfillSessionPresence(
    options?: { projectRoot?: string },
    session?: Session,
  ): Promise<{ scanned: number; synced: number }> {
    return this.withWriteLock(async () => {
      const db = this.getDb(session);
      const sessions = options?.projectRoot
        ? await this.listSessionsForProjectRoot(options.projectRoot, undefined, session)
        : await this.listSessions(undefined, session);
      let synced = 0;

      for (const codexSession of sessions) {
        const existingPresence = await db.one(
          app.codex_session_presence.where({ session_id: codexSession.session_id }),
        );
        if (existingPresence) {
          continue;
        }
        const [turns, syncState] = await Promise.all([
          db.all(app.codex_turns.where({ session_id: codexSession.session_id }).orderBy("sequence", "asc")),
          db.one(app.codex_sync_states.where({ absolute_path: codexSession.rollout_path })),
        ]);
        await this.upsertSessionPresence(
          db,
          codexSession,
          projectionFromStoredSession(codexSession, turns),
          syncState?.synced_at ?? codexSession.updated_at,
        );
        synced += 1;
      }

      return {
        scanned: sessions.length,
        synced,
      };
    });
  }

  async listCompletionEvents(
    options?: {
      completedAfter?: TimestampInput;
      limit?: number;
    },
    session?: Session,
  ): Promise<CodexCompletionEvent[]> {
    const db = this.getDb(session);
    const completedAfter = options?.completedAfter
      ? asDate(options.completedAfter)
      : undefined;
    const turns = await db.all(
      completedAfter
        ? app.codex_turns
            .where({
              status: "completed",
              completed_at: { gte: completedAfter },
            })
            .orderBy("completed_at", "asc")
            .limit(clampLimit(options?.limit, 50))
        : app.codex_turns
            .where({ status: "completed" })
            .orderBy("completed_at", "asc")
            .limit(clampLimit(options?.limit, 50)),
    );
    const sessionCache = new Map<string, CodexSession | null>();
    const completions: CodexCompletionEvent[] = [];

    for (const turn of turns) {
      if (!turn.completed_at) {
        continue;
      }
      let codexSession = sessionCache.get(turn.session_id);
      if (codexSession === undefined) {
        codexSession = await this.getCodexSessionByExternalId(db, turn.session_id);
        sessionCache.set(turn.session_id, codexSession);
      }
      if (!codexSession) {
        continue;
      }
      const projectPath = codexSession.project_root ?? codexSession.cwd;
      completions.push({
        id: `${turn.session_id}-${turn.turn_id}`,
        sessionId: turn.session_id,
        turnId: turn.turn_id,
        projectPath,
        projectName: basename(projectPath),
        source: codexSession.source ?? codexSession.originator ?? "codex",
        summary: turn.assistant_message ?? turn.assistant_partial ?? undefined,
        status: turn.status,
        timestamp: turn.completed_at,
        completedAt: turn.completed_at,
        updatedAt: turn.updated_at,
      });
    }

    return completions;
  }

  async getSessionSummary(
    sessionId: string,
    session?: Session,
  ): Promise<CodexSessionSummary | null> {
    const db = this.getDb(session);
    const codexSession = await this.getCodexSessionByExternalId(db, sessionId);
    if (!codexSession) {
      return null;
    }

    const [turns, syncState, presence] = await Promise.all([
      db.all(app.codex_turns.where({ session_id: sessionId }).orderBy("sequence", "asc")),
      db.one(app.codex_sync_states.where({ absolute_path: codexSession.rollout_path })),
      db.one(app.codex_session_presence.where({ session_id: sessionId })),
    ]);

    return {
      session: normalizeRow(codexSession),
      turns: turns.map((turn) => normalizeRow(turn)),
      syncState: syncState ? normalizeRow(syncState) : null,
      presence: presence ? normalizeRow(presence) : null,
    };
  }

  async getSession(sessionId: string, session?: Session): Promise<CodexSession | null> {
    return this.getCodexSessionByExternalId(this.getDb(session), sessionId);
  }

  async getSyncStateByPath(
    absolutePath: string,
    session?: Session,
  ): Promise<CodexSyncState | null> {
    const row = await this.getDb(session).one(app.codex_sync_states.where({ absolute_path: absolutePath }));
    return row ? normalizeRow(row) : null;
  }

  async listSyncStates(session?: Session): Promise<CodexSyncState[]> {
    const rows = await this.getDb(session).all(app.codex_sync_states.orderBy("synced_at", "desc"));
    return rows.map((row) => normalizeRow(row));
  }

  async upsertJAgentDefinition(
    input: UpsertJAgentDefinitionInput,
    session?: Session,
  ): Promise<JAgentDefinition> {
    const db = this.getDb(session);
    const existing = await this.getJAgentDefinitionByExternalId(db, input.definitionId);
    const payload: JAgentDefinitionInit = {
      definition_id: input.definitionId,
      name: input.name,
      version: input.version,
      source_kind: input.sourceKind,
      entrypoint: input.entrypoint,
      metadata_json: nullable(input.metadataJson),
      created_at: asDate(input.createdAt),
      updated_at: asDate(input.updatedAt),
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_definitions, existing.id, payload);
      return this.requireJAgentDefinition(db, input.definitionId);
    }

    return db.insertDurable(app.j_agent_definitions, payload, { tier: this.writeTier });
  }

  async recordJAgentRunStarted(
    input: RecordJAgentRunStartedInput,
    session?: Session,
  ): Promise<JAgentRun> {
    const db = this.getDb(session);
    const definition = await this.requireJAgentDefinition(db, input.definitionId);
    const parentSession = input.parentSessionId
      ? await this.getCodexSessionByExternalId(db, input.parentSessionId)
      : null;
    const initiatorSession = input.initiatorSessionId
      ? await this.getCodexSessionByExternalId(db, input.initiatorSessionId)
      : null;
    const existing = await this.getJAgentRunByExternalId(db, input.runId);
    const startedAt = asDate(input.startedAt);
    const updatedAt = asDate(input.updatedAt ?? input.startedAt);
    const payload: JAgentRunInit = {
      run_id: input.runId,
      definition_id: input.definitionId,
      definition_row_id: definition.id,
      status: input.status ?? "running",
      project_root: input.projectRoot,
      repo_root: nullable(input.repoRoot),
      cwd: nullable(input.cwd),
      trigger_source: nullable(input.triggerSource),
      parent_session_id: nullable(input.parentSessionId),
      parent_session_row_id: nullable(parentSession?.id),
      parent_turn_id: nullable(input.parentTurnId),
      initiator_session_id: nullable(input.initiatorSessionId),
      initiator_session_row_id: nullable(initiatorSession?.id),
      requested_role: nullable(input.requestedRole),
      requested_model: nullable(input.requestedModel),
      requested_reasoning_effort: nullable(input.requestedReasoningEffort),
      fork_turns: nullable(input.forkTurns),
      current_step_key: nullable(input.currentStepKey),
      input_json: nullable(input.inputJson),
      output_json: null,
      error_text: null,
      started_at: startedAt,
      updated_at: updatedAt,
      completed_at: null,
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_runs, existing.id, payload);
      return this.requireJAgentRun(db, input.runId);
    }

    return db.insertDurable(app.j_agent_runs, payload, { tier: this.writeTier });
  }

  async recordJAgentRunCompleted(
    input: RecordJAgentRunCompletedInput,
    session?: Session,
  ): Promise<JAgentRun> {
    const db = this.getDb(session);
    const existing = await this.requireJAgentRun(db, input.runId);
    const completedAt = asDate(input.completedAt);
    await this.updateRow(db, app.j_agent_runs, existing.id, {
      status: input.status ?? "completed",
      output_json: input.outputJson,
      error_text: input.errorText,
      current_step_key: input.currentStepKey,
      updated_at: asDate(input.updatedAt ?? input.completedAt),
      completed_at: completedAt,
    });
    return this.requireJAgentRun(db, input.runId);
  }

  async recordJAgentStepStarted(
    input: RecordJAgentStepStartedInput,
    session?: Session,
  ): Promise<JAgentStep> {
    const db = this.getDb(session);
    const run = await this.requireJAgentRun(db, input.runId);
    const existing = await this.getJAgentStepByExternalId(db, input.runId, input.stepId);
    const startedAt = asDate(input.startedAt);
    const updatedAt = asDate(input.updatedAt ?? input.startedAt);
    const payload: JAgentStepInit = {
      step_id: input.stepId,
      run_id: input.runId,
      run_row_id: run.id,
      sequence: input.sequence,
      step_key: input.stepKey,
      step_kind: input.stepKind,
      status: input.status ?? "running",
      input_json: nullable(input.inputJson),
      output_json: null,
      error_text: null,
      started_at: startedAt,
      updated_at: updatedAt,
      completed_at: null,
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_steps, existing.id, payload);
    } else {
      await db.insertDurable(app.j_agent_steps, payload, { tier: this.writeTier });
    }

    await this.updateRow(db, app.j_agent_runs, run.id, {
      current_step_key: input.stepKey,
      updated_at: updatedAt,
    });
    return this.requireJAgentStep(db, input.runId, input.stepId);
  }

  async recordJAgentStepCompleted(
    input: RecordJAgentStepCompletedInput,
    session?: Session,
  ): Promise<JAgentStep> {
    const db = this.getDb(session);
    const step = await this.requireJAgentStep(db, input.runId, input.stepId);
    const updatedAt = asDate(input.updatedAt ?? input.completedAt);
    await this.updateRow(db, app.j_agent_steps, step.id, {
      status: input.status ?? "completed",
      output_json: input.outputJson,
      error_text: input.errorText,
      updated_at: updatedAt,
      completed_at: asDate(input.completedAt),
    });
    return this.requireJAgentStep(db, input.runId, input.stepId);
  }

  async recordJAgentAttemptStarted(
    input: RecordJAgentAttemptStartedInput,
    session?: Session,
  ): Promise<JAgentAttempt> {
    const db = this.getDb(session);
    const run = await this.requireJAgentRun(db, input.runId);
    const step = await this.requireJAgentStep(db, input.runId, input.stepId);
    const codexSession = input.codexSessionId
      ? await this.getCodexSessionByExternalId(db, input.codexSessionId)
      : null;
    const codexTurn =
      input.codexSessionId && input.codexTurnId
        ? await this.getCodexTurnByExternalId(db, input.codexSessionId, input.codexTurnId)
        : null;
    const attemptId = input.attemptId ?? randomUUID();
    const existing = await db.one(app.j_agent_attempts.where({ attempt_id: attemptId }));
    const payload: JAgentAttemptInit = {
      attempt_id: attemptId,
      run_id: input.runId,
      run_row_id: run.id,
      step_id: input.stepId,
      step_row_id: step.id,
      attempt: input.attempt,
      status: input.status ?? "running",
      codex_session_id: nullable(input.codexSessionId),
      codex_session_row_id: nullable(codexSession?.id),
      codex_turn_id: nullable(input.codexTurnId),
      codex_turn_row_id: nullable(codexTurn?.id),
      fork_turns: nullable(input.forkTurns),
      model_name: nullable(input.modelName),
      reasoning_effort: nullable(input.reasoningEffort),
      started_at: asDate(input.startedAt),
      completed_at: null,
      error_text: null,
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_attempts, existing.id, payload);
    } else {
      await db.insertDurable(app.j_agent_attempts, payload, { tier: this.writeTier });
    }

    const attempt = await db.one(app.j_agent_attempts.where({ attempt_id: attemptId }));
    if (!attempt) {
      throw new Error(`j agent attempt ${attemptId} missing after upsert`);
    }
    return attempt;
  }

  async recordJAgentAttemptCompleted(
    input: RecordJAgentAttemptCompletedInput,
    session?: Session,
  ): Promise<JAgentAttempt> {
    const db = this.getDb(session);
    const attempt = await db.one(app.j_agent_attempts.where({ attempt_id: input.attemptId }));
    if (!attempt || attempt.run_id !== input.runId || attempt.step_id !== input.stepId) {
      throw new Error(
        `j agent attempt ${input.attemptId} not found for run ${input.runId} step ${input.stepId}`,
      );
    }
    await this.updateRow(db, app.j_agent_attempts, attempt.id, {
      status: input.status ?? "completed",
      completed_at: asDate(input.completedAt),
      error_text: input.errorText,
    });
    const updated = await db.one(app.j_agent_attempts.where({ attempt_id: input.attemptId }));
    if (!updated) {
      throw new Error(`j agent attempt ${input.attemptId} missing after update`);
    }
    return updated;
  }

  async recordJAgentWaitStarted(
    input: RecordJAgentWaitStartedInput,
    session?: Session,
  ): Promise<JAgentWait> {
    const db = this.getDb(session);
    const run = await this.requireJAgentRun(db, input.runId);
    const step = await this.requireJAgentStep(db, input.runId, input.stepId);
    const targetSession = input.targetSessionId
      ? await this.getCodexSessionByExternalId(db, input.targetSessionId)
      : null;
    const targetTurn =
      input.targetSessionId && input.targetTurnId
        ? await this.getCodexTurnByExternalId(db, input.targetSessionId, input.targetTurnId)
        : null;
    const waitId = input.waitId ?? randomUUID();
    const existing = await db.one(app.j_agent_waits.where({ wait_id: waitId }));
    const payload: JAgentWaitInit = {
      wait_id: waitId,
      run_id: input.runId,
      run_row_id: run.id,
      step_id: input.stepId,
      step_row_id: step.id,
      wait_kind: input.waitKind,
      target_session_id: nullable(input.targetSessionId),
      target_session_row_id: nullable(targetSession?.id),
      target_turn_id: nullable(input.targetTurnId),
      target_turn_row_id: nullable(targetTurn?.id),
      resume_condition_json: nullable(input.resumeConditionJson),
      status: input.status ?? "waiting",
      started_at: asDate(input.startedAt),
      resumed_at: null,
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_waits, existing.id, payload);
    } else {
      await db.insertDurable(app.j_agent_waits, payload, { tier: this.writeTier });
    }

    const wait = await db.one(app.j_agent_waits.where({ wait_id: waitId }));
    if (!wait) {
      throw new Error(`j agent wait ${waitId} missing after upsert`);
    }
    return wait;
  }

  async recordJAgentWaitResolved(
    input: RecordJAgentWaitResolvedInput,
    session?: Session,
  ): Promise<JAgentWait> {
    const db = this.getDb(session);
    const wait = await db.one(app.j_agent_waits.where({ wait_id: input.waitId }));
    if (!wait || wait.run_id !== input.runId) {
      throw new Error(`j agent wait ${input.waitId} not found for run ${input.runId}`);
    }
    await this.updateRow(db, app.j_agent_waits, wait.id, {
      status: input.status ?? "resolved",
      resumed_at: asDate(input.resumedAt),
    });
    const updated = await db.one(app.j_agent_waits.where({ wait_id: input.waitId }));
    if (!updated) {
      throw new Error(`j agent wait ${input.waitId} missing after update`);
    }
    return updated;
  }

  async bindJAgentSession(
    input: BindJAgentSessionInput,
    session?: Session,
  ): Promise<JAgentSessionBinding> {
    const db = this.getDb(session);
    const run = await this.requireJAgentRun(db, input.runId);
    const codexSession = await this.requireCodexSession(db, input.codexSessionId);
    const parentSession = input.parentSessionId
      ? await this.getCodexSessionByExternalId(db, input.parentSessionId)
      : null;
    const existing = input.bindingId
      ? await db.one(app.j_agent_session_bindings.where({ binding_id: input.bindingId }))
      : await db.one(
          app.j_agent_session_bindings.where({
            run_id: input.runId,
            codex_session_id: input.codexSessionId,
            binding_role: input.bindingRole,
          }),
        );
    const bindingId = input.bindingId ?? existing?.binding_id ?? randomUUID();
    const payload: JAgentSessionBindingInit = {
      binding_id: bindingId,
      run_id: input.runId,
      run_row_id: run.id,
      codex_session_id: input.codexSessionId,
      codex_session_row_id: codexSession.id,
      binding_role: input.bindingRole,
      parent_session_id: nullable(input.parentSessionId),
      parent_session_row_id: nullable(parentSession?.id),
      created_at: asDate(input.createdAt),
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_session_bindings, existing.id, payload);
    } else {
      await db.insertDurable(app.j_agent_session_bindings, payload, { tier: this.writeTier });
    }

    const binding = await db.one(app.j_agent_session_bindings.where({ binding_id: bindingId }));
    if (!binding) {
      throw new Error(`j agent session binding ${bindingId} missing after upsert`);
    }
    return binding;
  }

  async recordJAgentArtifact(
    input: RecordJAgentArtifactInput,
    session?: Session,
  ): Promise<JAgentArtifact> {
    const db = this.getDb(session);
    const run = await this.requireJAgentRun(db, input.runId);
    const step = input.stepId ? await this.getJAgentStepByExternalId(db, input.runId, input.stepId) : null;
    const artifactId = input.artifactId ?? randomUUID();
    const existing = await db.one(app.j_agent_artifacts.where({ artifact_id: artifactId }));
    const payload: JAgentArtifactInit = {
      artifact_id: artifactId,
      run_id: input.runId,
      run_row_id: run.id,
      step_id: nullable(input.stepId),
      step_row_id: nullable(step?.id),
      kind: input.kind,
      path: input.path,
      text_preview: nullable(input.textPreview),
      metadata_json: nullable(input.metadataJson),
      created_at: asDate(input.createdAt),
    };

    if (existing) {
      await this.updateRow(db, app.j_agent_artifacts, existing.id, payload);
    } else {
      await db.insertDurable(app.j_agent_artifacts, payload, { tier: this.writeTier });
    }

    const artifact = await db.one(app.j_agent_artifacts.where({ artifact_id: artifactId }));
    if (!artifact) {
      throw new Error(`j agent artifact ${artifactId} missing after upsert`);
    }
    return artifact;
  }

  async listActiveJAgentRuns(
    options?: { projectRoot?: string; limit?: number },
    session?: Session,
  ): Promise<JAgentRun[]> {
    const runs = await this.getDb(session).all(
      app.j_agent_runs.orderBy("updated_at", "desc"),
    );
    const filtered = runs.filter(
      (run) =>
        !TERMINAL_AGENT_RUN_STATUSES.has(run.status) &&
        (!options?.projectRoot || run.project_root === options.projectRoot),
    );
    return options?.limit === undefined
      ? filtered
      : filtered.slice(0, clampLimit(options.limit, 50));
  }

  async listJAgentRunsForSession(
    sessionId: string,
    options?: { limit?: number },
    session?: Session,
  ): Promise<JAgentRun[]> {
    const db = this.getDb(session);
    const bindings = await db.all(
      app.j_agent_session_bindings
        .where({ codex_session_id: sessionId })
        .orderBy("created_at", "desc"),
    );
    const runIds = uniqueStrings(bindings.map((binding) => binding.run_id));
    let runs = await Promise.all(
      runIds.map(async (runId) => this.getJAgentRunByExternalId(db, runId)),
    );
    let visibleRuns = runs.filter((run): run is JAgentRun => !!run);
    const nativeRunId = nativeCodexSessionRunId(sessionId);
    const hasNativeRun = visibleRuns.some((run) => run.run_id === nativeRunId);
    if (!hasNativeRun && (await this.ensureNativeCodexSessionRun(sessionId, session))) {
      const refreshedBindings = await db.all(
        app.j_agent_session_bindings
          .where({ codex_session_id: sessionId })
          .orderBy("created_at", "desc")
          .limit(clampLimit(options?.limit, 50)),
      );
      const refreshedRunIds = uniqueStrings(refreshedBindings.map((binding) => binding.run_id));
      runs = await Promise.all(
        refreshedRunIds.map(async (runId) => this.getJAgentRunByExternalId(db, runId)),
      );
      visibleRuns = runs.filter((run): run is JAgentRun => !!run);
    }
    const sortedRuns = visibleRuns
      .filter((run): run is JAgentRun => !!run)
      .sort((left, right) => right.started_at.getTime() - left.started_at.getTime());
    return options?.limit === undefined
      ? sortedRuns
      : sortedRuns.slice(0, clampLimit(options.limit, 50));
  }

  async ensureNativeCodexSessionRun(
    sessionId: string,
    session?: Session,
  ): Promise<boolean> {
    return this.withWriteLock(async () => {
      const db = this.getDb(session);
      const codexSession = await this.getCodexSessionByExternalId(db, sessionId);
      if (!codexSession) {
        return false;
      }

      const runId = nativeCodexSessionRunId(sessionId);
      const [existingRun, existingBinding] = await Promise.all([
        this.getJAgentRunByExternalId(db, runId),
        db.one(
          app.j_agent_session_bindings.where({
            binding_id: `${runId}:primary-session`,
          }),
        ),
      ]);
      if (existingRun && existingBinding) {
        return false;
      }

      await this.upsertNativeCodexSessionRunFromSession(db, codexSession, session);
      return true;
    });
  }

  async getJAgentRunSummary(
    runId: string,
    session?: Session,
  ): Promise<JAgentRunSummary | null> {
    const db = this.getDb(session);
    const run = await this.getJAgentRunByExternalId(db, runId);
    if (!run) {
      return null;
    }

    const definition = await this.requireJAgentDefinition(db, run.definition_id);
    const [steps, attempts, waits, sessionBindings, artifacts] = await Promise.all([
      db.all(app.j_agent_steps.where({ run_id: runId }).orderBy("sequence", "asc")),
      db.all(app.j_agent_attempts.where({ run_id: runId }).orderBy("attempt", "asc")),
      db.all(app.j_agent_waits.where({ run_id: runId }).orderBy("started_at", "asc")),
      db.all(app.j_agent_session_bindings.where({ run_id: runId }).orderBy("created_at", "asc")),
      db.all(app.j_agent_artifacts.where({ run_id: runId }).orderBy("created_at", "asc")),
    ]);

    const boundSessionIds = uniqueStrings(
      sessionBindings.map((binding) => binding.codex_session_id),
    );
    const boundSessions = await Promise.all(
      boundSessionIds.map(async (sessionId) => this.getCodexSessionByExternalId(db, sessionId)),
    );

    return {
      definition,
      run,
      steps,
      attempts,
      waits,
      sessionBindings,
      artifacts,
      boundSessions: boundSessions.filter(
        (codexSession): codexSession is CodexSession => !!codexSession,
      ),
    };
  }

  private getDb(session?: Session): Db {
    return session ? this.context.forSession(session, app) : this.context.db(app);
  }

  private async withWriteLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.writeQueue;
    let release!: () => void;
    this.writeQueue = new Promise<void>((resolve) => {
      release = resolve;
    });

    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  private async upsertSession(db: Db, projection: CodexSessionProjection): Promise<CodexSession> {
    const existing = await this.getCodexSessionByExternalId(db, projection.sessionId);
    const derivedLastUserAt = latestProjectionDate(
      projection.turns
        .filter((turn) => !!turn.userMessage)
        .map((turn) => turn.updatedAt ?? turn.startedAt),
    );
    const derivedLastAssistantAt = latestProjectionDate(
      projection.turns
        .filter((turn) => !!turn.assistantMessage || !!turn.assistantPartial)
        .map((turn) => turn.updatedAt ?? turn.completedAt ?? turn.startedAt),
    );
    const derivedLastCompletionAt = latestProjectionDate(
      projection.turns.map((turn) => turn.completedAt),
    );
    const payload: CodexSessionInit = {
      session_id: projection.sessionId,
      rollout_path: projection.rolloutPath,
      cwd: projection.cwd,
      project_root: projection.projectRoot ?? projection.cwd,
      repo_root: nullable(projection.repoRoot),
      git_branch: nullable(projection.gitBranch),
      originator: nullable(projection.originator),
      source: nullable(projection.source),
      cli_version: nullable(projection.cliVersion),
      model_provider: nullable(projection.modelProvider),
      model_name: nullable(projection.modelName),
      reasoning_effort: nullable(projection.reasoningEffort),
      agent_nickname: nullable(projection.agentNickname),
      agent_role: nullable(projection.agentRole),
      agent_path: nullable(projection.agentPath),
      first_user_message: nullable(projection.firstUserMessage),
      latest_user_message: nullable(projection.latestUserMessage),
      latest_assistant_message: nullable(projection.latestAssistantMessage),
      latest_assistant_partial: nullable(projection.latestAssistantPartial),
      latest_preview: nullable(sessionPreview(projection)),
      status: projection.status,
      created_at: asDate(projection.createdAt),
      updated_at: asDate(projection.updatedAt),
      latest_activity_at: asDate(projection.latestActivityAt ?? projection.updatedAt),
      last_user_at: projection.lastUserAt
        ? asDate(projection.lastUserAt)
        : nullable(derivedLastUserAt),
      last_assistant_at: projection.lastAssistantAt
        ? asDate(projection.lastAssistantAt)
        : nullable(derivedLastAssistantAt),
      last_completion_at: projection.lastCompletionAt
        ? asDate(projection.lastCompletionAt)
        : nullable(derivedLastCompletionAt),
      metadata_json: nullable(projection.metadataJson),
    };

    if (existing) {
      await this.updateRow(db, app.codex_sessions, existing.id, payload);
      return this.requireCodexSession(db, projection.sessionId);
    }

    return db.insertDurable(app.codex_sessions, payload, { tier: this.writeTier });
  }

  private async upsertNativeCodexSessionRunFromSession(
    db: Db,
    codexSession: CodexSession,
    session?: Session,
  ): Promise<void> {
    const runId = nativeCodexSessionRunId(codexSession.session_id);
    const startedAt = codexSession.created_at;
    const updatedAt = codexSession.latest_activity_at ?? codexSession.updated_at;
    const status = nativeCodexSessionRunStatus(codexSession.status);

    await this.upsertJAgentDefinition(
      {
        definitionId: NATIVE_CODEX_SESSION_DEFINITION_ID,
        name: NATIVE_CODEX_SESSION_DEFINITION_NAME,
        version: NATIVE_CODEX_SESSION_DEFINITION_VERSION,
        sourceKind: NATIVE_CODEX_SESSION_SOURCE_KIND,
        entrypoint: NATIVE_CODEX_SESSION_ENTRYPOINT,
        metadataJson: {
          kind: "native_codex_session",
          source: "codex-rollout-projector",
        },
        createdAt: startedAt,
        updatedAt,
      },
      session,
    );

    await this.recordJAgentRunStarted(
      {
        runId,
        definitionId: NATIVE_CODEX_SESSION_DEFINITION_ID,
        status,
        projectRoot: codexSession.project_root,
        repoRoot: codexSession.repo_root ?? undefined,
        cwd: codexSession.cwd ?? undefined,
        triggerSource: NATIVE_CODEX_SESSION_TRIGGER_SOURCE,
        initiatorSessionId: codexSession.session_id,
        requestedRole: codexSession.agent_role ?? codexSession.agent_nickname ?? "codex",
        requestedModel: codexSession.model_name ?? undefined,
        requestedReasoningEffort: codexSession.reasoning_effort ?? undefined,
        inputJson: {
          sessionId: codexSession.session_id,
          rolloutPath: codexSession.rollout_path,
          originator: codexSession.originator ?? null,
          source: codexSession.source ?? null,
        },
        startedAt,
        updatedAt,
      },
      session,
    );

    if (TERMINAL_AGENT_RUN_STATUSES.has(status)) {
      await this.recordJAgentRunCompleted(
        {
          runId,
          status,
          outputJson: {
            latestPreview: codexSession.latest_preview ?? null,
            latestAssistantMessage: codexSession.latest_assistant_message ?? null,
          },
          completedAt: codexSession.last_completion_at ?? updatedAt,
          updatedAt,
        },
        session,
      );
    }

    await this.bindJAgentSession(
      {
        runId,
        codexSessionId: codexSession.session_id,
        bindingRole: NATIVE_CODEX_SESSION_BINDING_ROLE,
        bindingId: `${runId}:primary-session`,
        createdAt: startedAt,
      },
      session,
    );
  }

  private async upsertTurns(
    db: Db,
    codexSession: CodexSession,
    turns: CodexTurnProjection[],
  ): Promise<void> {
    const existingTurns = await db.all(app.codex_turns.where({ session_id: codexSession.session_id }));
    const existingByTurnId = new Map(existingTurns.map((turn) => [turn.turn_id, turn]));
    const seenTurnIds = new Set<string>();

    for (const turn of turns) {
      seenTurnIds.add(turn.turnId);
      const payload: CodexTurnInit = {
        turn_id: turn.turnId,
        session_id: codexSession.session_id,
        session_row_id: codexSession.id,
        sequence: turn.sequence,
        status: turn.status,
        user_message: nullable(turn.userMessage),
        assistant_message: nullable(turn.assistantMessage),
        assistant_partial: nullable(turn.assistantPartial),
        plan_text: nullable(turn.planText),
        reasoning_summary: nullable(turn.reasoningSummary),
        started_at: turn.startedAt ? asDate(turn.startedAt) : null,
        completed_at: turn.completedAt ? asDate(turn.completedAt) : null,
        duration_ms: nullable(turn.durationMs),
        updated_at: asDate(turn.updatedAt ?? codexSession.updated_at),
      };
      const existing = existingByTurnId.get(turn.turnId);
      if (existing) {
        await this.updateRow(db, app.codex_turns, existing.id, payload);
        continue;
      }
      await db.insertDurable(app.codex_turns, payload, { tier: this.writeTier });
    }

    for (const turn of existingTurns) {
      if (seenTurnIds.has(turn.turn_id)) {
        continue;
      }
      await db.deleteDurable(app.codex_turns, turn.id, { tier: this.writeTier });
    }
  }

  private async upsertSyncState(
    db: Db,
    codexSession: CodexSession,
    syncState: CodexSyncStateInput,
  ): Promise<CodexSyncState> {
    const existing = await db.one(
      app.codex_sync_states.where({ absolute_path: syncState.absolutePath }),
    );
    const payload: CodexSyncStateInit = {
      source_id: syncState.sourceId,
      absolute_path: syncState.absolutePath,
      session_id: nullable(syncState.sessionId ?? codexSession.session_id),
      session_row_id: nullable(codexSession.id),
      line_count: syncState.lineCount,
      synced_at: asDate(syncState.syncedAt),
    };

    if (existing) {
      await this.updateRow(db, app.codex_sync_states, existing.id, payload);
      const updated = await db.one(
        app.codex_sync_states.where({ absolute_path: syncState.absolutePath }),
      );
      if (!updated) {
        throw new Error(`sync state ${syncState.absolutePath} missing after update`);
      }
      return updated;
    }

    return db.insertDurable(app.codex_sync_states, payload, { tier: this.writeTier });
  }

  private async upsertSessionPresence(
    db: Db,
    codexSession: CodexSession,
    projection: CodexSessionProjection,
    lastSyncedAt: Date,
  ): Promise<CodexSessionPresence> {
    const existing = await db.one(
      app.codex_session_presence.where({ session_id: codexSession.session_id }),
    );
    const currentTurnProjection = activeProjectionTurn(projection.turns);
    const currentTurn = currentTurnProjection
      ? await this.getCodexTurnByExternalId(db, codexSession.session_id, currentTurnProjection.turnId)
      : null;
    const payload: CodexSessionPresenceInit = {
      session_id: codexSession.session_id,
      session_row_id: codexSession.id,
      project_root: codexSession.project_root,
      repo_root: nullable(codexSession.repo_root),
      cwd: codexSession.cwd,
      state: derivePresenceState(projection),
      current_turn_id: nullable(currentTurnProjection?.turnId),
      current_turn_row_id: nullable(currentTurn?.id),
      current_turn_status: nullable(currentTurnProjection?.status),
      started_at: codexSession.created_at,
      latest_activity_at: codexSession.latest_activity_at,
      last_event_at: presenceLastEventAt(projection),
      last_user_at: nullable(codexSession.last_user_at),
      last_assistant_at: nullable(codexSession.last_assistant_at),
      last_completion_at: nullable(codexSession.last_completion_at),
      last_synced_at: lastSyncedAt,
      updated_at: codexSession.updated_at,
    };

    if (existing) {
      await this.updateRow(db, app.codex_session_presence, existing.id, payload);
    } else {
      await db.insertDurable(app.codex_session_presence, payload, { tier: this.writeTier });
    }

    const presence = await db.one(
      app.codex_session_presence.where({ session_id: codexSession.session_id }),
    );
    if (!presence) {
      throw new Error(`codex session presence ${codexSession.session_id} missing after upsert`);
    }
    return presence;
  }

  private async getCodexSessionByExternalId(
    db: Db,
    sessionId: string,
  ): Promise<CodexSession | null> {
    const row = await db.one(app.codex_sessions.where({ session_id: sessionId }));
    return row ? normalizeRow(row) : null;
  }

  private async requireCodexSession(db: Db, sessionId: string): Promise<CodexSession> {
    const codexSession = await this.getCodexSessionByExternalId(db, sessionId);
    if (!codexSession) {
      throw new Error(`codex session ${sessionId} not found`);
    }
    return codexSession;
  }

  private async getCodexTurnByExternalId(
    db: Db,
    sessionId: string,
    turnId: string,
  ): Promise<CodexTurn | null> {
    const row = await db.one(app.codex_turns.where({ session_id: sessionId, turn_id: turnId }));
    return row ? normalizeRow(row) : null;
  }

  private async getJAgentDefinitionByExternalId(
    db: Db,
    definitionId: string,
  ): Promise<JAgentDefinition | null> {
    const row = await db.one(app.j_agent_definitions.where({ definition_id: definitionId }));
    return row ? normalizeRow(row) : null;
  }

  private async requireJAgentDefinition(
    db: Db,
    definitionId: string,
  ): Promise<JAgentDefinition> {
    const definition = await this.getJAgentDefinitionByExternalId(db, definitionId);
    if (!definition) {
      throw new Error(`j agent definition ${definitionId} not found`);
    }
    return definition;
  }

  private async getJAgentRunByExternalId(db: Db, runId: string): Promise<JAgentRun | null> {
    const row = await db.one(app.j_agent_runs.where({ run_id: runId }));
    return row ? normalizeRow(row) : null;
  }

  private async requireJAgentRun(db: Db, runId: string): Promise<JAgentRun> {
    const run = await this.getJAgentRunByExternalId(db, runId);
    if (!run) {
      throw new Error(`j agent run ${runId} not found`);
    }
    return run;
  }

  private async getJAgentStepByExternalId(
    db: Db,
    runId: string,
    stepId: string,
  ): Promise<JAgentStep | null> {
    const row = await db.one(app.j_agent_steps.where({ run_id: runId, step_id: stepId }));
    return row ? normalizeRow(row) : null;
  }

  private async requireJAgentStep(
    db: Db,
    runId: string,
    stepId: string,
  ): Promise<JAgentStep> {
    const step = await this.getJAgentStepByExternalId(db, runId, stepId);
    if (!step) {
      throw new Error(`j agent step ${stepId} not found for run ${runId}`);
    }
    return step;
  }

  private async updateRow<T, Init>(
    db: Db,
    table: TableProxy<T, Init>,
    id: string,
    updates: Partial<Init>,
  ): Promise<void> {
    const payload = pruneUndefined(updates as Record<string, unknown>);
    if (Object.keys(payload).length === 0) {
      return;
    }
    await db.updateDurable(table as never, id, payload as never, { tier: this.writeTier });
  }
}

export function createCodexSessionStore(
  config: CodexSessionStoreConfig,
): CodexSessionStore {
  const tier = config.tier ?? "edge";
  const context = createJazzContext({
    appId: config.appId ?? DEFAULT_APP_ID,
    app,
    driver: { type: "persistent", dataPath: config.dataPath },
    env: config.env ?? "dev",
    userBranch: config.userBranch ?? "main",
    serverUrl: config.serverUrl,
    serverPathPrefix: config.serverPathPrefix,
    backendSecret: config.backendSecret,
    adminSecret: config.adminSecret,
    tier,
  });
  return new CodexSessionStore(context, tier);
}

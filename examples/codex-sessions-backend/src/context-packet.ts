import { createHash } from "node:crypto";
import { basename } from "node:path";
import type {
  CodexSession,
  CodexStreamEvent,
  CodexSyncState,
  CodexTurn,
  ProjectContextEntry,
} from "../schema/app.js";
import type {
  CodexSessionPresenceSummary,
  CodexSessionStore,
  CodexSessionSummary,
} from "./store.js";

const DEFAULT_SINCE = "8h";
const DEFAULT_ACTIVE_LIMIT = 10;
const DEFAULT_CONTEXT_LIMIT = 20;
const DEFAULT_RECENT_SESSION_LIMIT = 20;
const DEFAULT_TURN_LIMIT_PER_SESSION = 2;
const DEFAULT_STREAM_CURSOR_LIMIT = 20;
const DEFAULT_PREVIEW_CHARS = 600;

export interface BuildProjectContextPacketOptions {
  projectRoot: string;
  since?: string | Date;
  now?: string | Date;
  activeLimit?: number;
  contextLimit?: number;
  recentSessionLimit?: number;
  turnLimitPerSession?: number;
  streamCursorLimit?: number;
  maxPreviewChars?: number;
  includeSectionBodies?: boolean;
}

export interface GetProjectContextPacketSectionOptions extends BuildProjectContextPacketOptions {
  nodeId: string;
}

export interface ProjectContextPacket {
  schemaVersion: "codex.project-context-packet.v1";
  generatedAt: string;
  projectRoot: string;
  query: {
    since: string;
    sinceAt: string;
    activeLimit: number;
    contextLimit: number;
    recentSessionLimit: number;
    turnLimitPerSession: number;
    streamCursorLimit: number;
    maxPreviewChars: number;
  };
  document: ProjectContextDocument;
  cursors: ProjectContextCursors;
  evidenceRefs: string[];
  sections?: ProjectContextPacketSection[];
}

export interface ProjectContextDocument {
  docId: string;
  docName: string;
  docDescription: string;
  retrievalModel: "pageindex-structure-first";
  structure: ProjectContextIndexNode[];
}

export interface ProjectContextIndexNode {
  title: string;
  nodeId: string;
  lineNum: number;
  summary: string;
  refs: string[];
}

export interface ProjectContextPacketSection extends ProjectContextIndexNode {
  body: string;
  items: ProjectContextPacketItem[];
}

export interface ProjectContextPacketItem {
  kind: string;
  title: string;
  summary: string;
  refs: string[];
  updatedAt?: string;
  status?: string;
  metadata?: Record<string, unknown>;
}

export interface ProjectContextCursors {
  stream: ProjectContextStreamCursor[];
  rollouts: ProjectContextRolloutCursor[];
}

export interface ProjectContextStreamCursor {
  sessionId: string;
  afterSequence: number;
  eventId: string;
  observedAt: string;
}

export interface ProjectContextRolloutCursor {
  sessionId: string;
  rolloutPath: string;
  lineCount: number;
  syncedAt: string;
}

interface NormalizedOptions {
  projectRoot: string;
  sinceLabel: string;
  sinceAt: Date;
  now: Date;
  activeLimit: number;
  contextLimit: number;
  recentSessionLimit: number;
  turnLimitPerSession: number;
  streamCursorLimit: number;
  maxPreviewChars: number;
  includeSectionBodies: boolean;
}

export async function buildProjectContextPacket(
  store: CodexSessionStore,
  options: BuildProjectContextPacketOptions,
): Promise<ProjectContextPacket> {
  const normalized = normalizeOptions(options);
  const activeSessions = await store.listActiveSessionSummaries({
    projectRoot: normalized.projectRoot,
    limit: normalized.activeLimit,
  });
  const recentSessions = (await store.listSessionsForProjectRoot(
    normalized.projectRoot,
    normalized.recentSessionLimit * 4,
  ))
    .filter((session) => isAtOrAfter(session.latest_activity_at ?? session.updated_at, normalized.sinceAt))
    .slice(0, normalized.recentSessionLimit);
  const contextEntries = (await store.listProjectContextEntries({
    projectRoot: normalized.projectRoot,
    limit: normalized.contextLimit * 2,
  }))
    .filter((entry) => isAtOrAfter(entry.updated_at, normalized.sinceAt))
    .slice(0, normalized.contextLimit);

  const detailSessionIds = Array.from(new Set([
    ...activeSessions.map((summary) => summary.session.session_id),
    ...recentSessions.map((session) => session.session_id),
  ]));
  const sessionDetails = await Promise.all(
    detailSessionIds.map((sessionId) => store.getSessionSummary(sessionId)),
  );
  const summaries = sessionDetails.filter((summary): summary is CodexSessionSummary => summary !== null);
  const summaryBySessionId = new Map(
    summaries.map((summary) => [summary.session.session_id, summary]),
  );
  const activeSection = buildActiveSessionsSection(activeSessions, summaryBySessionId, normalized);
  const recentTurnsSection = buildRecentTurnsSection(
    summaries,
    normalized,
  );
  const projectContextSection = buildProjectContextSection(contextEntries, normalized);
  const cursorSessions = dedupeSessions([
    ...activeSessions.map((summary) => summary.session),
    ...recentSessions,
  ]);
  const streamCursorSection = await buildStreamCursorSection(store, cursorSessions, normalized);
  const sections = [
    activeSection,
    recentTurnsSection,
    projectContextSection,
    streamCursorSection,
  ];
  const rolloutCursors = collectRolloutCursors(
    summaries,
  );
  const evidenceRefs = Array.from(new Set(sections.flatMap((section) => section.refs)));

  return {
    schemaVersion: "codex.project-context-packet.v1",
    generatedAt: normalized.now.toISOString(),
    projectRoot: normalized.projectRoot,
    query: {
      since: normalized.sinceLabel,
      sinceAt: normalized.sinceAt.toISOString(),
      activeLimit: normalized.activeLimit,
      contextLimit: normalized.contextLimit,
      recentSessionLimit: normalized.recentSessionLimit,
      turnLimitPerSession: normalized.turnLimitPerSession,
      streamCursorLimit: normalized.streamCursorLimit,
      maxPreviewChars: normalized.maxPreviewChars,
    },
    document: {
      docId: `project-context:${shortHash(normalized.projectRoot)}`,
      docName: `Codex context for ${basename(normalized.projectRoot) || normalized.projectRoot}`,
      docDescription: "Structure-first indexed context packet backed by Jazz2 session facts.",
      retrievalModel: "pageindex-structure-first",
      structure: sections.map(({ body: _body, items: _items, ...node }) => node),
    },
    cursors: {
      stream: streamCursorSection.items.map((item) => item.metadata)
        .filter(isStreamCursorMetadata)
        .map((metadata) => ({
          sessionId: metadata.sessionId,
          afterSequence: metadata.afterSequence,
          eventId: metadata.eventId,
          observedAt: metadata.observedAt,
        })),
      rollouts: rolloutCursors,
    },
    evidenceRefs,
    sections: normalized.includeSectionBodies ? sections : undefined,
  };
}

export async function getProjectContextPacketSection(
  store: CodexSessionStore,
  options: GetProjectContextPacketSectionOptions,
): Promise<ProjectContextPacketSection | null> {
  const packet = await buildProjectContextPacket(store, {
    ...options,
    includeSectionBodies: true,
  });
  return packet.sections?.find((section) => section.nodeId === options.nodeId) ?? null;
}

export function parseSinceDuration(value: string, now: Date = new Date()): Date {
  const trimmed = value.trim();
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/i.exec(trimmed);
  if (!match) {
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
    throw new Error("since must be an ISO timestamp or duration like 30m, 8h, or 2d");
  }

  const amount = Number(match[1]);
  const unit = match[2].toLowerCase();
  const multiplier = unit === "ms"
    ? 1
    : unit === "s"
      ? 1_000
      : unit === "m"
        ? 60_000
        : unit === "h"
          ? 60 * 60_000
          : 24 * 60 * 60_000;
  return new Date(now.getTime() - amount * multiplier);
}

function normalizeOptions(options: BuildProjectContextPacketOptions): NormalizedOptions {
  const now = options.now ? new Date(options.now) : new Date();
  if (Number.isNaN(now.getTime())) {
    throw new Error("now must be a valid timestamp");
  }
  const sinceLabel = options.since instanceof Date
    ? options.since.toISOString()
    : options.since ?? DEFAULT_SINCE;
  const sinceAt = options.since instanceof Date
    ? options.since
    : parseSinceDuration(sinceLabel, now);
  if (Number.isNaN(sinceAt.getTime())) {
    throw new Error("since must be a valid timestamp");
  }

  return {
    projectRoot: options.projectRoot,
    sinceLabel,
    sinceAt,
    now,
    activeLimit: clamp(options.activeLimit, DEFAULT_ACTIVE_LIMIT, 100),
    contextLimit: clamp(options.contextLimit, DEFAULT_CONTEXT_LIMIT, 100),
    recentSessionLimit: clamp(options.recentSessionLimit, DEFAULT_RECENT_SESSION_LIMIT, 100),
    turnLimitPerSession: clamp(options.turnLimitPerSession, DEFAULT_TURN_LIMIT_PER_SESSION, 10),
    streamCursorLimit: clamp(options.streamCursorLimit, DEFAULT_STREAM_CURSOR_LIMIT, 100),
    maxPreviewChars: clamp(options.maxPreviewChars, DEFAULT_PREVIEW_CHARS, 4_000),
    includeSectionBodies: options.includeSectionBodies === true,
  };
}

function buildActiveSessionsSection(
  activeSessions: CodexSessionPresenceSummary[],
  summaryBySessionId: Map<string, CodexSessionSummary>,
  options: NormalizedOptions,
): ProjectContextPacketSection {
  const items = activeSessions.map((summary) => {
    const text = summary.currentTurn?.user_message
      ?? summary.session.latest_user_message
      ?? summary.session.latest_preview
      ?? summary.presence.state;
    const refs = refsForSession(
      summary.session,
      summaryBySessionId.get(summary.session.session_id)?.syncState ?? null,
    );
    return {
      kind: "active-session",
      title: summary.session.session_id,
      summary: truncateText(text, options.maxPreviewChars),
      refs,
      updatedAt: toIso(summary.presence.last_event_at ?? summary.session.latest_activity_at),
      status: summary.presence.state,
      metadata: {
        cwd: summary.presence.cwd ?? summary.session.cwd,
        currentTurnId: summary.presence.current_turn_id ?? null,
      },
    };
  });
  return section(
    "0001",
    "Active Sessions",
    items.length === 0
      ? "No fresh active Codex sessions are indexed for this project."
      : `${items.length} fresh active Codex session(s) are indexed for this project.`,
    items,
  );
}

function buildRecentTurnsSection(
  summaries: CodexSessionSummary[],
  options: NormalizedOptions,
): ProjectContextPacketSection {
  const items = summaries.flatMap((summary) => {
    const recentTurns = summary.turns
      .filter((turn) => isAtOrAfter(turn.updated_at, options.sinceAt))
      .slice(-options.turnLimitPerSession);
    return recentTurns.map((turn) => turnItem(summary.session, summary.syncState, turn, options));
  });
  return section(
    "0002",
    "Recent Turns",
    items.length === 0
      ? "No recent Codex turns matched the project/time window."
      : `${items.length} bounded recent turn preview(s), newest session scan first.`,
    items,
  );
}

function buildProjectContextSection(
  entries: ProjectContextEntry[],
  options: NormalizedOptions,
): ProjectContextPacketSection {
  const items = entries.map((entry) => ({
    kind: entry.source_kind,
    title: `${entry.provider}:${entry.session_id}`,
    summary: truncateText(entry.summary, options.maxPreviewChars),
    refs: [
      `${entry.provider}:${entry.session_id}`,
      `context:${entry.context_id}`,
      `watermark:${entry.source_watermark}`,
    ],
    updatedAt: entry.updated_at.toISOString(),
    status: entry.status,
    metadata: {
      turnId: entry.turn_id ?? null,
      confidence: entry.confidence ?? null,
      score: entry.score ?? null,
      body: entry.body ? truncateText(entry.body, options.maxPreviewChars) : null,
      metadataJson: entry.metadata_json ?? null,
    },
  }));
  return section(
    "0003",
    "Project Context",
    items.length === 0
      ? "No ready project-context digest matched the project/time window."
      : `${items.length} project-context digest(s) are available for targeted expansion.`,
    items,
  );
}

async function buildStreamCursorSection(
  store: CodexSessionStore,
  recentSessions: CodexSession[],
  options: NormalizedOptions,
): Promise<ProjectContextPacketSection> {
  const items: ProjectContextPacketItem[] = [];
  for (const session of recentSessions.slice(0, options.streamCursorLimit)) {
    const latest = await store.listCodexStreamEvents({
      sessionId: session.session_id,
      latest: true,
      limit: 1,
    });
    const event = latest[0];
    if (!event) {
      continue;
    }
    items.push(streamCursorItem(event));
  }
  return section(
    "0004",
    "Stream Cursors",
    items.length === 0
      ? "No replicated stream cursor is indexed yet; callers should fall back to rollout offsets."
      : `${items.length} per-session stream cursor(s) allow future reads to fetch only deltas.`,
    items,
  );
}

function turnItem(
  session: CodexSession,
  syncState: CodexSyncState | null,
  turn: CodexTurn,
  options: NormalizedOptions,
): ProjectContextPacketItem {
  const assistantText = turn.assistant_message ?? turn.assistant_partial;
  const summaryParts = [
    turn.user_message ? `user: ${turn.user_message}` : null,
    assistantText ? `assistant: ${assistantText}` : null,
  ].filter((part): part is string => part !== null);
  return {
    kind: "turn",
    title: `${session.session_id}:${turn.turn_id}`,
    summary: truncateText(summaryParts.join("\n"), options.maxPreviewChars),
    refs: [...refsForSession(session, syncState), `turn:${session.session_id}:${turn.turn_id}`],
    updatedAt: turn.updated_at.toISOString(),
    status: turn.status,
    metadata: {
      sequence: turn.sequence,
      completedAt: turn.completed_at?.toISOString() ?? null,
    },
  };
}

function streamCursorItem(event: CodexStreamEvent): ProjectContextPacketItem {
  const metadata = {
    sessionId: event.session_id,
    afterSequence: event.sequence,
    eventId: event.event_id,
    observedAt: event.observed_at.toISOString(),
  };
  return {
    kind: "stream-cursor",
    title: event.session_id,
    summary: `Next stream read can start after sequence ${event.sequence}.`,
    refs: [`codex:${event.session_id}`, `stream-event:${event.event_id}`],
    updatedAt: event.observed_at.toISOString(),
    status: event.event_kind,
    metadata,
  };
}

function dedupeSessions(sessions: CodexSession[]): CodexSession[] {
  const seen = new Set<string>();
  const deduped: CodexSession[] = [];
  for (const session of sessions) {
    if (seen.has(session.session_id)) {
      continue;
    }
    seen.add(session.session_id);
    deduped.push(session);
  }
  return deduped;
}

function section(
  nodeId: string,
  title: string,
  summary: string,
  items: ProjectContextPacketItem[],
): ProjectContextPacketSection {
  const refs = Array.from(new Set(items.flatMap((item) => item.refs)));
  const body = items.length === 0
    ? summary
    : items.map((item) => formatItem(item)).join("\n\n");
  return {
    title,
    nodeId,
    lineNum: Number.parseInt(nodeId, 10),
    summary,
    refs,
    body,
    items,
  };
}

function formatItem(item: ProjectContextPacketItem): string {
  const lines = [
    `## ${item.title}`,
    `kind: ${item.kind}`,
    item.status ? `status: ${item.status}` : null,
    item.updatedAt ? `updatedAt: ${item.updatedAt}` : null,
    `refs: ${item.refs.join(", ")}`,
    "",
    item.summary,
  ].filter((line): line is string => line !== null);
  return lines.join("\n");
}

function collectRolloutCursors(summaries: CodexSessionSummary[]): ProjectContextRolloutCursor[] {
  return summaries
    .filter((summary) => summary.syncState !== null)
    .map((summary) => ({
      sessionId: summary.session.session_id,
      rolloutPath: summary.syncState?.absolute_path ?? summary.session.rollout_path,
      lineCount: summary.syncState?.line_count ?? 0,
      syncedAt: toIso(summary.syncState?.synced_at ?? summary.session.updated_at),
    }));
}

function refsForSession(session: CodexSession, syncState: CodexSyncState | null): string[] {
  return [
    `codex:${session.session_id}`,
    `rollout:${syncState?.absolute_path ?? session.rollout_path}#line=${syncState?.line_count ?? 0}`,
  ];
}

function isAtOrAfter(value: Date | undefined, floor: Date): boolean {
  return value !== undefined && value.getTime() >= floor.getTime();
}

function toIso(value: Date | undefined): string {
  return (value ?? new Date(0)).toISOString();
}

function truncateText(value: string, maxChars: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
}

function clamp(value: number | undefined, fallback: number, max: number): number {
  if (value === undefined) {
    return fallback;
  }
  if (!Number.isFinite(value)) {
    throw new Error("numeric packet limits must be finite");
  }
  return Math.min(Math.max(1, Math.trunc(value)), max);
}

function shortHash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 12);
}

function isStreamCursorMetadata(value: unknown): value is {
  sessionId: string;
  afterSequence: number;
  eventId: string;
  observedAt: string;
} {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const metadata = value as Record<string, unknown>;
  return typeof metadata.sessionId === "string"
    && typeof metadata.afterSequence === "number"
    && typeof metadata.eventId === "string"
    && typeof metadata.observedAt === "string";
}

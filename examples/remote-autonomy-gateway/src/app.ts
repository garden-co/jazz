import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { Elysia, type AnyElysia } from "elysia";
import {
  createAgentDataStore,
  type AgentClaimRecord,
  type AgentDataStore,
  type AgentDataStoreConfig,
  type JobRecord,
  type JobStatus,
  type JsonValue as AgentJsonValue,
} from "../../agent-infra-backend/src/index.js";
import {
  type CodexStreamEvent,
  type CodexSessionPresence,
  type JsonValue as CodexJsonValue,
} from "../../codex-sessions-backend/schema/app.js";
import {
  createCodexSessionStore as createCodexSessionDataStore,
  type CodexSessionStore,
  type CodexSessionStoreConfig,
} from "../../codex-sessions-backend/src/index.js";

type JsonObject = Record<string, unknown>;
type SyncProbeResult = {
  ok: boolean;
  status: string;
  latencyMs?: number;
  error?: string;
};

export interface RemoteAutonomyGatewayOptions {
  agentDataPath?: string;
  codexDataPath?: string;
  agentAppId?: string;
  codexAppId?: string;
  syncServerUrl?: string;
  syncServerAppId?: string;
  syncServerPathPrefix?: string;
  backendSecret?: string;
  adminSecret?: string;
  hostId?: string;
  env?: string;
  userBranch?: string;
  port?: number;
  connectStoresToSyncServer?: boolean;
  syncServerProbeTimeoutMs?: number;
  syncServerProbe?: () => Promise<SyncProbeResult>;
}

export interface RemoteAutonomyGateway {
  app: AnyElysia;
  close: () => Promise<void>;
}

class GatewayError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

const SERVICE_NAME = "remote-autonomy-gateway";
const CONTROL_RUN_ID = "remote-autonomy-gateway:control";
const DEFAULT_SYNC_SERVER_URL =
  "https://nikitavoloboev-jazz2-sync-ingress.tailbf2c6c.ts.net";
const DEFAULT_SYNC_SERVER_APP_ID = "313aa802-8598-5165-bb91-dab72dcb9d46";

export function createRemoteAutonomyGateway(
  options: RemoteAutonomyGatewayOptions = {},
): RemoteAutonomyGateway {
  const resolved = resolveOptions(options);
  const agentStore = createAgentDataStore(agentStoreConfig(resolved));
  const codexStore = createCodexSessionDataStore(codexStoreConfig(resolved));
  let controlRunReady: Promise<void> | null = null;

  const ensureControlRun = async () => {
    if (!controlRunReady) {
      controlRunReady = agentStore
        .recordRunStarted({
          runId: CONTROL_RUN_ID,
          agentId: SERVICE_NAME,
          requestSummary: "Remote autonomy gateway control trace",
          status: "running",
          startedAt: new Date(),
          contextJson: {
            hostId: resolved.hostId,
            syncServerUrl: resolved.syncServerUrl,
            syncServerAppId: resolved.syncServerAppId,
          },
          agent: {
            lane: "remote-autonomy",
            promptSurface: "elysia-http",
            status: "active",
          },
        })
        .then(() => undefined);
    }
    await controlRunReady;
  };

  const recordGatewayEvent = async (
    eventType: string,
    summaryText: string,
    payloadJson: JsonObject,
  ) => {
    await ensureControlRun();
    await agentStore.appendSemanticEvent({
      runId: CONTROL_RUN_ID,
      eventId: `${eventType}:${randomUUID()}`,
      eventType,
      summaryText,
      payloadJson: payloadJson as AgentJsonValue,
      occurredAt: new Date(),
    });
  };

  const app = new Elysia({ name: SERVICE_NAME })
    .onError(({ error, set }) => {
      const status = error instanceof GatewayError ? error.status : 500;
      set.status = status;
      return {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      };
    })
    .get("/health", async () => {
      const [syncServer, stores] = await Promise.all([
        resolved.syncServerProbe(),
        checkStores(agentStore, codexStore),
      ]);
      void recordGatewayEvent("gateway_health", "health probe", {
        syncServer,
        stores,
      }).catch(() => undefined);
      return {
        ok: syncServer.ok && stores.agentInfra.ok && stores.codexSessions.ok,
        service: SERVICE_NAME,
        hostId: resolved.hostId,
        timestamp: new Date().toISOString(),
        syncServer: {
          url: resolved.syncServerUrl,
          appId: resolved.syncServerAppId,
          ...syncServer,
        },
        stores,
      };
    })
    .get("/v1/bootstrap", () => ({
      ok: true,
      service: SERVICE_NAME,
      hostId: resolved.hostId,
      syncServer: {
        url: resolved.syncServerUrl,
        appId: resolved.syncServerAppId,
        pathPrefix: resolved.syncServerPathPrefix,
      },
      stores: {
        agentDataPath: resolved.agentDataPath,
        codexDataPath: resolved.codexDataPath,
        connectedToSyncServer: resolved.connectStoresToSyncServer,
      },
      endpoints: {
        health: "/health",
        state: "/v1/state",
        codexPresence: "/v1/codex/presence",
        codexSessions: "/v1/codex/sessions",
        codexStreamEvents: "/v1/codex/stream-events",
        syncJobs: "/v1/sync/jobs",
        syncReceipts: "/v1/sync/receipts",
        claims: "/v1/claims",
      },
    }))
    .get("/v1/state", async ({ query }) => {
      const limit = intQuery(query.limit, 20);
      const [sessions, jobs, claims] = await Promise.all([
        codexStore.listActiveSessionSummaries({ limit }),
        agentStore.listJobs({ includeFinished: false, limit }),
        agentStore.listAgentClaims({ limit }),
      ]);
      return {
        ok: true,
        hostId: resolved.hostId,
        syncServer: {
          url: resolved.syncServerUrl,
          appId: resolved.syncServerAppId,
        },
        sessions: sessions.map(serializePresenceSummary),
        jobs: serialize(jobs),
        claims: serialize(claims),
      };
    })
    .post("/v1/codex/presence", async ({ body }) => {
      const payload = objectBody(body);
      const summary = await codexStore.recordTerminalPresence({
        terminalSessionId: requiredString(payload, "terminalSessionId"),
        sessionId: optionalString(payload, "sessionId"),
        turnId: optionalString(payload, "turnId"),
        cwd: optionalString(payload, "cwd"),
        projectRoot: optionalString(payload, "projectRoot"),
        repoRoot: optionalString(payload, "repoRoot"),
        state: optionalString(payload, "state"),
        activityPath: optionalString(payload, "activityPath"),
        active: optionalBoolean(payload, "active"),
        stale: optionalBoolean(payload, "stale"),
        updatedAt: optionalTimestamp(payload, "updatedAt"),
        startedAt: optionalTimestamp(payload, "startedAt"),
        updatedAtMs: optionalNumber(payload, "updatedAtMs"),
        startedAtMs: optionalNumber(payload, "startedAtMs"),
        pid: optionalNumber(payload, "pid"),
        runtimePid: optionalNumber(payload, "runtimePid"),
        runtimeTty: optionalString(payload, "runtimeTty"),
        runtimeHost: optionalString(payload, "runtimeHost") ?? resolved.hostId,
      });
      await recordGatewayEvent("codex_presence", "Codex terminal presence recorded", {
        sessionId: summary.session.session_id,
        terminalSessionId: payload.terminalSessionId,
        state: summary.presence?.state,
        hostId: resolved.hostId,
      });
      return {
        ok: true,
        ...serialize(summary),
      };
    })
    .post("/v1/codex/stream-events", async ({ body }) => {
      const payload = objectBody(body);
      const event = await codexStore.recordCodexStreamEvent({
        eventId: optionalString(payload, "eventId"),
        sessionId: requiredString(payload, "sessionId"),
        turnId: optionalString(payload, "turnId"),
        sequence: requiredNumber(payload, "sequence"),
        eventKind: requiredString(payload, "eventKind"),
        eventType: optionalString(payload, "eventType"),
        sourceId: optionalString(payload, "sourceId"),
        sourceHost: optionalString(payload, "sourceHost") ?? resolved.hostId,
        sourcePath: optionalString(payload, "sourcePath"),
        textDelta: optionalString(payload, "textDelta"),
        payloadJson: optionalJson(payload, "payloadJson") as CodexJsonValue | undefined,
        rawJson: optionalJson(payload, "rawJson") as CodexJsonValue | undefined,
        schemaHash: optionalString(payload, "schemaHash"),
        createdAt: optionalTimestamp(payload, "createdAt"),
        observedAt: optionalTimestamp(payload, "observedAt"),
      });
      await recordGatewayEvent("codex_stream_event", "Codex stream event recorded", {
        eventId: event.event_id,
        sessionId: event.session_id,
        turnId: event.turn_id,
        sequence: event.sequence,
        eventKind: event.event_kind,
        sourceHost: event.source_host,
      });
      return { ok: true, event: serializeStreamEvent(event) };
    })
    .get("/v1/codex/stream-events", async ({ query }) => {
      const events = await codexStore.listCodexStreamEvents({
        sessionId: optionalQueryString(query.sessionId),
        turnId: optionalQueryString(query.turnId),
        afterSequence: intQueryOptional(query.afterSequence),
        limit: intQuery(query.limit, 200),
      });
      return {
        ok: true,
        events: events.map(serializeStreamEvent),
      };
    })
    .get("/v1/codex/sessions", async ({ query }) => {
      const active = truthy(query.active);
      const limit = intQuery(query.limit, 20);
      const projectRoot = optionalQueryString(query.projectRoot);
      if (active) {
        const sessions = await codexStore.listActiveSessionSummaries({
          projectRoot,
          limit,
        });
        return {
          ok: true,
          sessions: sessions.map(serializePresenceSummary),
        };
      }
      const sessions = projectRoot
        ? await codexStore.listSessionsForProjectRoot(projectRoot, limit)
        : await codexStore.listSessions(limit);
      return {
        ok: true,
        sessions: serialize(sessions),
      };
    })
    .get("/v1/codex/sessions/:sessionId", async ({ params }) => {
      const summary = await codexStore.getSessionSummary(params.sessionId);
      if (!summary) {
        throw new GatewayError(404, `Codex session ${params.sessionId} not found`);
      }
      return {
        ok: true,
        ...serialize(summary),
      };
    })
    .post("/v1/sync/jobs", async ({ body }) => {
      const payload = objectBody(body);
      const job = await agentStore.recordJob({
        jobId: optionalString(payload, "jobId"),
        kind: requiredString(payload, "kind"),
        repoRoot: optionalString(payload, "repoRoot"),
        workspaceRoot: optionalString(payload, "workspaceRoot"),
        sourceChatKind: optionalString(payload, "sourceChatKind"),
        dedupeKey: optionalString(payload, "dedupeKey"),
        targetSession: optionalString(payload, "targetSession"),
        targetTurnWatermark: optionalString(payload, "targetTurnWatermark"),
        sourceSession: optionalString(payload, "sourceSession"),
        sourceWatermark: optionalString(payload, "sourceWatermark"),
        payloadJson: optionalJson(payload, "payloadJson") as AgentJsonValue | undefined,
        resultJson: optionalJson(payload, "resultJson") as AgentJsonValue | undefined,
        note: optionalString(payload, "note"),
        createdAt: optionalTimestamp(payload, "createdAt"),
      });
      await recordGatewayEvent("sync_job_recorded", "sync job recorded", {
        jobId: job.jobId,
        kind: job.kind,
        status: job.status,
      });
      return { ok: true, job: serializeJob(job) };
    })
    .get("/v1/sync/jobs", async ({ query }) => {
      const jobs = await agentStore.listJobs({
        kind: optionalQueryString(query.kind),
        status: optionalQueryString(query.status) as JobStatus | undefined,
        claimedBy: optionalQueryString(query.claimedBy),
        repoRoot: optionalQueryString(query.repoRoot),
        targetSession: optionalQueryString(query.targetSession),
        includeFinished: truthy(query.includeFinished),
        limit: intQuery(query.limit, 20),
      });
      return { ok: true, jobs: jobs.map(serializeJob) };
    })
    .post("/v1/sync/jobs/:jobId/claim", async ({ params, body }) => {
      const payload = objectBody(body);
      const job = await agentStore.claimJob({
        jobId: params.jobId,
        claimedBy: requiredString(payload, "claimedBy"),
        leaseExpiresAt: optionalTimestamp(payload, "leaseExpiresAt"),
        claimedAt: optionalTimestamp(payload, "claimedAt"),
        attempt: optionalNumber(payload, "attempt"),
        note: optionalString(payload, "note"),
      });
      await recordGatewayEvent("sync_job_claimed", "sync job claimed", {
        jobId: job.jobId,
        claimedBy: job.claimedBy,
        status: job.status,
      });
      return { ok: true, job: serializeJob(job) };
    })
    .post("/v1/sync/jobs/:jobId/status", async ({ params, body }) => {
      const payload = objectBody(body);
      const job = await agentStore.updateJob({
        jobId: params.jobId,
        status: requiredString(payload, "status") as JobStatus,
        claimedBy: optionalString(payload, "claimedBy"),
        leaseExpiresAt: optionalTimestamp(payload, "leaseExpiresAt"),
        attempt: optionalNumber(payload, "attempt"),
        resultJson: optionalJson(payload, "resultJson") as AgentJsonValue | undefined,
        note: optionalString(payload, "note"),
        updatedAt: optionalTimestamp(payload, "updatedAt"),
      });
      await recordGatewayEvent("sync_job_status", "sync job status updated", {
        jobId: job.jobId,
        status: job.status,
      });
      return { ok: true, job: serializeJob(job) };
    })
    .post("/v1/sync/receipts", async ({ body }) => {
      const payload = objectBody(body);
      const receipt = {
        receiptId: optionalString(payload, "receiptId") ?? randomUUID(),
        jobId: optionalString(payload, "jobId"),
        status: requiredString(payload, "status"),
        transport: requiredString(payload, "transport"),
        sourcePath: optionalString(payload, "sourcePath"),
        targetPath: optionalString(payload, "targetPath"),
        checksum: optionalString(payload, "checksum"),
        bytes: optionalNumber(payload, "bytes"),
        hostId: optionalString(payload, "hostId") ?? resolved.hostId,
        payloadJson: optionalJson(payload, "payloadJson"),
        recordedAt: new Date().toISOString(),
      };
      let job: JobRecord | null = null;
      if (receipt.jobId) {
        job = await agentStore.updateJob({
          jobId: receipt.jobId,
          status: receipt.status as JobStatus,
          resultJson: receipt as AgentJsonValue,
          note: `${receipt.transport} ${receipt.status}`,
          updatedAt: receipt.recordedAt,
        });
      }
      await recordGatewayEvent("sync_receipt", "sync receipt recorded", receipt);
      return {
        ok: true,
        receipt,
        job: job ? serializeJob(job) : null,
      };
    })
    .post("/v1/claims", async ({ body }) => {
      const payload = objectBody(body);
      const claim = await agentStore.recordAgentClaim({
        claimId: optionalString(payload, "claimId"),
        scope: requiredString(payload, "scope"),
        owner: requiredString(payload, "owner"),
        ownerSession: optionalString(payload, "ownerSession"),
        mode: optionalString(payload, "mode"),
        note: optionalString(payload, "note"),
        repoRoot: optionalString(payload, "repoRoot"),
        workspaceRoot: optionalString(payload, "workspaceRoot"),
        startedAt: optionalTimestamp(payload, "startedAt"),
        expiresAt: optionalTimestamp(payload, "expiresAt"),
        heartbeatAt: optionalTimestamp(payload, "heartbeatAt"),
      });
      await recordGatewayEvent("claim_recorded", "claim recorded", {
        claimId: claim.claimId,
        scope: claim.scope,
        owner: claim.owner,
        status: claim.status,
      });
      return { ok: true, claim: serializeClaim(claim) };
    })
    .get("/v1/claims", async ({ query }) => {
      const claims = await agentStore.listAgentClaims({
        scopePrefix: optionalQueryString(query.scopePrefix),
        ownerSession: optionalQueryString(query.ownerSession),
        includeReleased: truthy(query.includeReleased),
        includeExpired: truthy(query.includeExpired),
        limit: intQuery(query.limit, 20),
      });
      return { ok: true, claims: claims.map(serializeClaim) };
    });

  return {
    app,
    close: async () => {
      await Promise.allSettled([
        agentStore.shutdown(),
        codexStore.shutdown(),
      ]);
    },
  };
}

async function checkStores(
  agentStore: AgentDataStore,
  codexStore: CodexSessionStore,
) {
  const [agentInfra, codexSessions] = await Promise.all([
    checkStore(async () => {
      await agentStore.listJobs({ limit: 1 });
    }),
    checkStore(async () => {
      await codexStore.listSessionPresence({ limit: 1 });
    }),
  ]);
  return { agentInfra, codexSessions };
}

async function checkStore(probe: () => Promise<void>) {
  const started = Date.now();
  try {
    await probe();
    return { ok: true, latencyMs: Date.now() - started };
  } catch (error) {
    return {
      ok: false,
      latencyMs: Date.now() - started,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function resolveOptions(options: RemoteAutonomyGatewayOptions) {
  const root = join(homedir(), ".jazz2", "remote-autonomy");
  const syncServerUrl = stripTrailingSlash(
    options.syncServerUrl
      ?? process.env.REMOTE_AUTONOMY_SYNC_SERVER_URL
      ?? DEFAULT_SYNC_SERVER_URL,
  );
  const connectStoresToSyncServer = options.connectStoresToSyncServer
    ?? truthy(process.env.REMOTE_AUTONOMY_CONNECT_SYNC ?? "1");
  return {
    agentDataPath:
      options.agentDataPath
      ?? process.env.REMOTE_AUTONOMY_AGENT_DATA_PATH
      ?? join(root, "agent-infra.db"),
    codexDataPath:
      options.codexDataPath
      ?? process.env.REMOTE_AUTONOMY_CODEX_DATA_PATH
      ?? join(root, "codex-sessions.db"),
    agentAppId:
      options.agentAppId
      ?? process.env.REMOTE_AUTONOMY_AGENT_APP_ID
      ?? "run-agent-infra",
    codexAppId:
      options.codexAppId
      ?? process.env.REMOTE_AUTONOMY_CODEX_APP_ID
      ?? "codex-sessions",
    syncServerUrl,
    syncServerAppId:
      options.syncServerAppId
      ?? process.env.REMOTE_AUTONOMY_SYNC_SERVER_APP_ID
      ?? DEFAULT_SYNC_SERVER_APP_ID,
    syncServerPathPrefix:
      options.syncServerPathPrefix
      ?? process.env.REMOTE_AUTONOMY_SYNC_SERVER_PATH_PREFIX,
    backendSecret:
      options.backendSecret ?? process.env.REMOTE_AUTONOMY_BACKEND_SECRET,
    adminSecret:
      options.adminSecret ?? process.env.REMOTE_AUTONOMY_ADMIN_SECRET,
    hostId:
      options.hostId
      ?? process.env.REMOTE_AUTONOMY_HOST_ID
      ?? process.env.HOST
      ?? "unknown-host",
    env: options.env ?? process.env.REMOTE_AUTONOMY_ENV ?? "remote-autonomy",
    userBranch:
      options.userBranch
      ?? process.env.REMOTE_AUTONOMY_USER_BRANCH
      ?? "main",
    port:
      options.port
      ?? (process.env.REMOTE_AUTONOMY_PORT
        ? Number(process.env.REMOTE_AUTONOMY_PORT)
        : 7474),
    connectStoresToSyncServer,
    syncServerProbeTimeoutMs:
      options.syncServerProbeTimeoutMs
      ?? (process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS
        ? Number(process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS)
        : 3_000),
    syncServerProbe:
      options.syncServerProbe
      ?? (() =>
        probeSyncServer(
          syncServerUrl,
          options.syncServerProbeTimeoutMs
            ?? (process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS
              ? Number(process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS)
              : 3_000),
        )),
  };
}

type ResolvedOptions = ReturnType<typeof resolveOptions>;

function agentStoreConfig(options: ResolvedOptions): AgentDataStoreConfig {
  return storeConfig({
    dataPath: options.agentDataPath,
    appId: options.agentAppId,
    options,
  });
}

function codexStoreConfig(options: ResolvedOptions): CodexSessionStoreConfig {
  return storeConfig({
    dataPath: options.codexDataPath,
    appId: options.codexAppId,
    options,
  });
}

function storeConfig<T extends AgentDataStoreConfig | CodexSessionStoreConfig>(
  input: {
    dataPath: string;
    appId: string;
    options: ResolvedOptions;
  },
): T {
  mkdirSync(dirname(input.dataPath), { recursive: true });
  return {
    dataPath: input.dataPath,
    appId: input.appId,
    env: input.options.env,
    userBranch: input.options.userBranch,
    serverUrl: input.options.connectStoresToSyncServer
      ? input.options.syncServerUrl
      : undefined,
    serverPathPrefix: input.options.syncServerPathPrefix,
    backendSecret: input.options.backendSecret,
    adminSecret: input.options.adminSecret,
    tier: input.options.connectStoresToSyncServer ? "edge" : "local",
  } as T;
}

async function probeSyncServer(
  syncServerUrl: string,
  timeoutMs: number,
): Promise<SyncProbeResult> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const started = Date.now();
  try {
    const response = await fetch(`${syncServerUrl}/health`, {
      signal: controller.signal,
    });
    const text = await response.text();
    let status = response.ok ? "healthy" : `http-${response.status}`;
    try {
      const json = JSON.parse(text) as { status?: unknown };
      if (typeof json.status === "string") status = json.status;
    } catch {
      // Plain text health bodies are accepted; the HTTP status is enough.
    }
    return {
      ok: response.ok,
      status,
      latencyMs: Date.now() - started,
    };
  } catch (error) {
    return {
      ok: false,
      status: "unreachable",
      latencyMs: Date.now() - started,
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    clearTimeout(timeout);
  }
}

function serializePresenceSummary(summary: {
  presence: CodexSessionPresence;
  session: { session_id: string; cwd?: string; latest_preview?: string };
  currentTurn: { turn_id: string; status: string } | null;
}) {
  return {
    sessionId: summary.presence.session_id,
    state: summary.presence.state,
    cwd: summary.presence.cwd,
    projectRoot: summary.presence.project_root,
    repoRoot: summary.presence.repo_root,
    currentTurnId: summary.presence.current_turn_id,
    currentTurnStatus: summary.presence.current_turn_status,
    lastEventAt: summary.presence.last_event_at?.toISOString(),
    lastSyncedAt: summary.presence.last_synced_at?.toISOString(),
    latestPreview: summary.session.latest_preview,
    currentTurn: summary.currentTurn ? serialize(summary.currentTurn) : null,
  };
}

function serializeJob(job: JobRecord) {
  return serialize(job);
}

function serializeClaim(claim: AgentClaimRecord) {
  return serialize(claim);
}

function serializeStreamEvent(event: CodexStreamEvent) {
  return {
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
    payloadJson: event.payload_json,
    rawJson: event.raw_json,
    schemaHash: event.schema_hash,
    createdAt: event.created_at.toISOString(),
    observedAt: event.observed_at.toISOString(),
  };
}

function serialize<T>(value: T): T {
  if (value instanceof Date) {
    return value.toISOString() as T;
  }
  if (Array.isArray(value)) {
    return value.map((item) => serialize(item)) as T;
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, serialize(entry)]),
    ) as T;
  }
  return value;
}

function objectBody(body: unknown): JsonObject {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new GatewayError(400, "expected JSON object body");
  }
  return body as JsonObject;
}

function requiredString(body: JsonObject, key: string): string {
  const value = body[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new GatewayError(400, `missing required string field ${key}`);
  }
  return value;
}

function optionalString(body: JsonObject, key: string): string | undefined {
  const value = body[key];
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value !== "string") {
    throw new GatewayError(400, `field ${key} must be a string`);
  }
  return value;
}

function optionalNumber(body: JsonObject, key: string): number | undefined {
  const value = body[key];
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new GatewayError(400, `field ${key} must be a finite number`);
  }
  return value;
}

function requiredNumber(body: JsonObject, key: string): number {
  const value = optionalNumber(body, key);
  if (value === undefined) {
    throw new GatewayError(400, `missing required number field ${key}`);
  }
  return value;
}

function optionalBoolean(body: JsonObject, key: string): boolean | undefined {
  const value = body[key];
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value !== "boolean") {
    throw new GatewayError(400, `field ${key} must be a boolean`);
  }
  return value;
}

function optionalTimestamp(
  body: JsonObject,
  key: string,
): Date | string | number | undefined {
  const value = body[key];
  if (value === undefined || value === null || value === "") return undefined;
  if (
    value instanceof Date
    || typeof value === "string"
    || typeof value === "number"
  ) {
    return value;
  }
  throw new GatewayError(400, `field ${key} must be a timestamp`);
}

function optionalJson(body: JsonObject, key: string): AgentJsonValue | CodexJsonValue | undefined {
  const value = body[key];
  if (value === undefined) return undefined;
  return value as AgentJsonValue;
}

function optionalQueryString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function intQuery(value: unknown, fallback: number): number {
  if (typeof value !== "string" || value.trim() === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function intQueryOptional(value: unknown): number | undefined {
  if (typeof value !== "string" || value.trim() === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function truthy(value: unknown): boolean {
  return value === true || value === "1" || value === "true" || value === "yes";
}

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

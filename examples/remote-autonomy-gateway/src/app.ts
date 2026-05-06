import { createHash, randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join, posix } from "node:path";
import { Elysia, type AnyElysia } from "elysia";
import {
  createAgentDataStore,
  type AgentClaimRecord,
  type AgentDataStore,
  type AgentDataStoreConfig,
  type JobRecord,
  type JobStatus,
  type JsonValue as AgentJsonValue,
  type SemanticEvent,
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
  localSpacesRoot?: string;
  remoteSpacesRoot?: string;
  objectStorageRegion?: string;
  objectStorageBucket?: string;
  designerSpacesPrefix?: string;
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
const DEFAULT_SYNC_SERVER_URL = "https://nikitavoloboev-jazz2-sync-ingress.tailbf2c6c.ts.net";
const DEFAULT_SYNC_SERVER_APP_ID = "313aa802-8598-5165-bb91-dab72dcb9d46";
const DEFAULT_REMOTE_HOME = "/users/nikiv";
const DEFAULT_OBJECT_STORAGE_REGION = "us-sanjose-1";
const DEFAULT_OBJECT_STORAGE_BUCKET = "x-sanjose";
const DEFAULT_DESIGNER_SPACES_PREFIX = "nikiv/designer";
const OBJECT_CACHE_DIR = ".object-cache";
const SPACE_WORK_DIR = "work";
const SPACE_SYNC_JOB_KIND = "space-rsync-mirror";
const SPACE_FILE_UPLOAD_JOB_KIND = "space-file-object-upload";
const SPACE_FILE_MATERIALIZE_JOB_KIND = "space-file-materialize";
const SPACE_FILE_EVENT_TYPE = "designer_space_file";

type ObjectStorageDescriptor = {
  provider: "oci";
  region: string;
  bucket: string;
  prefix: string;
  uri: string;
};

type DesignerSpaceRecord = {
  slug: string;
  title: string;
  localPath: string;
  remotePath: string;
  objectStoragePrefix: string;
  objectStorageUri: string;
  objectStorage: ObjectStorageDescriptor;
  syncKind: typeof SPACE_SYNC_JOB_KIND;
};

type SpaceFileMaterializeTarget = "local" | "remote";

type DesignerSpaceFileRecord = {
  objectRefId: string;
  spaceSlug: string;
  path: string;
  localPath: string;
  remotePath: string;
  contentHash: string;
  sizeBytes?: number;
  contentType?: string;
  revisionId?: string;
  writerId?: string;
  sourceSession?: string;
  objectStorage: ObjectStorageObjectDescriptor;
  uploadJobId?: string;
  materializeJobId?: string;
  materializeTarget: SpaceFileMaterializeTarget;
  updatedAt: string;
};

type DesignerSpaceFileListRecord = DesignerSpaceFileRecord & {
  contentBase64?: string;
};

type ObjectStorageObjectDescriptor = ObjectStorageDescriptor & {
  key: string;
};

type InlineSpaceFileReceipt = {
  objectCachePath: string;
  materializedPath: string;
  checksum: string;
  bytes: number;
  recordedAt: string;
};

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
    eventId?: string,
  ) => {
    await ensureControlRun();
    return await agentStore.appendSemanticEvent({
      runId: CONTROL_RUN_ID,
      eventId: eventId ?? `${eventType}:${randomUUID()}`,
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
        executorTraces: "/v1/executor/traces",
        syncJobs: "/v1/sync/jobs",
        syncReceipts: "/v1/sync/receipts",
        claims: "/v1/claims",
        spaces: "/v1/spaces",
        spaceFiles: "/v1/spaces/:slug/files",
        spaceSync: "/v1/spaces/:slug/sync",
      },
    }))
    .get("/v1/state", async ({ query }) => {
      const limit = intQuery(query.limit, 20);
      const [sessions, jobs, claims, spaces, spaceFiles] = await Promise.all([
        codexStore.listActiveSessionSummaries({ limit }),
        agentStore.listJobs({ includeFinished: false, limit }),
        agentStore.listAgentClaims({ limit }),
        listSpaceRecords(agentStore, limit),
        listSpaceFileRecords(agentStore, undefined, limit),
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
        spaces,
        spaceFiles,
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
    .post("/v1/executor/traces", async ({ body }) => {
      const payload = objectBody(body);
      const traceId =
        optionalString(payload, "trace_id") ??
        optionalString(payload, "traceId") ??
        optionalString(payload, "correlation_id") ??
        optionalString(payload, "correlationId") ??
        randomUUID();
      const executor = optionalString(payload, "executor") ?? "unknown";
      const eventType = optionalString(payload, "eventType") ?? "executor_trace";
      const status = optionalString(payload, "status") ?? "unknown";
      const event = await recordGatewayEvent(
        eventType,
        `${executor} executor trace ${status}`,
        {
          ...payload,
          traceId,
          executor,
          status,
          hostId: resolved.hostId,
          receivedAt: new Date().toISOString(),
        },
        `${eventType}:${executor}:${traceId}`,
      );
      return {
        ok: true,
        traceId,
        event: serializeSemanticEvent(event),
      };
    })
    .get("/v1/executor/traces", async ({ query }) => {
      const summary = await agentStore.getRunSummary(CONTROL_RUN_ID);
      const eventType = optionalQueryString(query.eventType);
      const executor = optionalQueryString(query.executor);
      const traceId = optionalQueryString(query.traceId) ?? optionalQueryString(query.trace_id);
      const limit = intQuery(query.limit, 50);
      const events = (summary?.semanticEvents ?? [])
        .filter((event) => !eventType || event.event_type === eventType)
        .filter((event) => {
          const payload = jsonObject(event.payload_json);
          if (!executor && !traceId) return true;
          if (executor && payload?.executor !== executor) return false;
          if (traceId && payload?.traceId !== traceId && payload?.trace_id !== traceId)
            return false;
          return true;
        })
        .slice(-Math.max(0, limit))
        .reverse();
      return {
        ok: true,
        events: events.map(serializeSemanticEvent),
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
    })
    .get("/v1/spaces", async ({ query }) => {
      return {
        ok: true,
        spaces: await listSpaceRecords(agentStore, intQuery(query.limit, 20)),
      };
    })
    .post("/v1/spaces", async ({ body }) => {
      const payload = objectBody(body);
      const space = resolveDesignerSpace(payload, resolved);
      const ownerSession = optionalString(payload, "ownerSession");
      const owner = optionalString(payload, "owner") ?? resolved.hostId;
      const job = await agentStore.recordJob({
        kind: SPACE_SYNC_JOB_KIND,
        repoRoot: space.remotePath,
        workspaceRoot: space.remotePath,
        sourceSession: ownerSession,
        dedupeKey: spaceJobDedupeKey(space.slug),
        payloadJson: {
          sourcePath: space.remotePath,
          targetPath: space.localPath,
          transport: "rsync",
          space,
        } as AgentJsonValue,
        note: `mirror Designer space ${space.slug}`,
      });
      const claim = await agentStore.recordAgentClaim({
        claimId: spaceClaimId(space.slug),
        scope: `space:${space.slug}`,
        owner,
        ownerSession,
        mode: "sync-owner",
        repoRoot: space.remotePath,
        workspaceRoot: space.remotePath,
        note: `Designer space ${space.slug} mirrors ${space.remotePath} to ${space.localPath}`,
      });
      await recordGatewayEvent("designer_space_registered", "Designer space registered", {
        slug: space.slug,
        localPath: space.localPath,
        remotePath: space.remotePath,
        objectStoragePrefix: space.objectStoragePrefix,
        objectStorage: space.objectStorage,
        jobId: job.jobId,
        claimId: claim.claimId,
      });
      return {
        ok: true,
        space,
        job: serializeJob(job),
        claim: serializeClaim(claim),
      };
    })
    .get("/v1/spaces/:slug/files", async ({ params, query }) => {
      const slug = spaceSlug(params.slug);
      return {
        ok: true,
        files: await listSpaceFileRecords(agentStore, slug, intQuery(query.limit, 50), {
          includeContent: truthy(query.includeContent),
          options: resolved,
        }),
      };
    })
    .post("/v1/spaces/:slug/files", async ({ params, body }) => {
      const slug = spaceSlug(params.slug);
      const payload = objectBody(body);
      const space = await requireSpaceRecord(agentStore, slug);
      const file = resolveDesignerSpaceFile(space, payload);
      const inlineContent = inlineSpaceFileContent(payload, file);
      const uploadJob = await agentStore.recordJob({
        kind: SPACE_FILE_UPLOAD_JOB_KIND,
        repoRoot: space.remotePath,
        workspaceRoot: space.remotePath,
        sourceSession: file.sourceSession,
        dedupeKey: spaceFileUploadDedupeKey(file),
        payloadJson: {
          action: "upload-object",
          sourcePath: optionalString(payload, "sourcePath") ?? file.localPath,
          targetUri: file.objectStorage.uri,
          file,
          objectStorage: file.objectStorage,
          space,
        } as AgentJsonValue,
        note: `upload ${file.path} for Designer space ${space.slug}`,
      });
      const materializeJob = await agentStore.recordJob({
        kind: SPACE_FILE_MATERIALIZE_JOB_KIND,
        repoRoot: space.remotePath,
        workspaceRoot: space.remotePath,
        sourceSession: file.sourceSession,
        dedupeKey: spaceFileMaterializeDedupeKey(file),
        payloadJson: {
          action: "materialize-object",
          sourceUri: file.objectStorage.uri,
          targetPath: file.materializeTarget === "remote" ? file.remotePath : file.localPath,
          target: file.materializeTarget,
          file,
          objectStorage: file.objectStorage,
          space,
        } as AgentJsonValue,
        note: `materialize ${file.path} for Designer space ${space.slug}`,
      });
      const recordedFile: DesignerSpaceFileRecord = {
        ...file,
        uploadJobId: uploadJob.jobId,
        materializeJobId: materializeJob.jobId,
      };
      let finalUploadJob = uploadJob;
      let finalMaterializeJob = materializeJob;
      let inlineReceipt: InlineSpaceFileReceipt | null = null;
      if (inlineContent) {
        inlineReceipt = await writeInlineSpaceFile(resolved, recordedFile, inlineContent);
        finalUploadJob = await agentStore.updateJob({
          jobId: uploadJob.jobId,
          status: "completed",
          resultJson: {
            status: "completed",
            transport: "inline-object-cache",
            targetPath: inlineReceipt.objectCachePath,
            checksum: recordedFile.contentHash,
            bytes: inlineContent.byteLength,
          } as AgentJsonValue,
          note: "inline object cached",
          updatedAt: inlineReceipt.recordedAt,
        });
        finalMaterializeJob = await agentStore.updateJob({
          jobId: materializeJob.jobId,
          status: "completed",
          resultJson: {
            status: "completed",
            transport: "inline-file",
            targetPath: inlineReceipt.materializedPath,
            checksum: recordedFile.contentHash,
            bytes: inlineContent.byteLength,
          } as AgentJsonValue,
          note: "inline file materialized",
          updatedAt: inlineReceipt.recordedAt,
        });
      }
      await recordGatewayEvent(
        SPACE_FILE_EVENT_TYPE,
        `Designer space file ${recordedFile.path} recorded`,
        {
          file: recordedFile,
          space,
          ...(inlineReceipt ? { inlineReceipt } : {}),
        },
        spaceFileEventId(recordedFile),
      );
      return {
        ok: true,
        file: recordedFile,
        uploadJob: serializeJob(finalUploadJob),
        materializeJob: serializeJob(finalMaterializeJob),
        inlineReceipt,
      };
    })
    .post("/v1/spaces/:slug/sync", async ({ params, body }) => {
      const slug = spaceSlug(params.slug);
      const payload = objectBody(body);
      const space = await requireSpaceRecord(agentStore, slug);
      const direction = syncDirection(optionalString(payload, "direction") ?? "pull");
      const job = await agentStore.recordJob({
        kind: SPACE_SYNC_JOB_KIND,
        repoRoot: space.remotePath,
        workspaceRoot: space.remotePath,
        sourceSession: optionalString(payload, "sourceSession"),
        dedupeKey: `${SPACE_SYNC_JOB_KIND}:${space.slug}:${direction}`,
        payloadJson: {
          direction,
          sourcePath: direction === "push" ? space.localPath : space.remotePath,
          targetPath: direction === "push" ? space.remotePath : space.localPath,
          transport: "rsync",
          space,
        } as AgentJsonValue,
        note: `${direction} Designer space ${space.slug}`,
      });
      await recordGatewayEvent("designer_space_sync_requested", "Designer space sync requested", {
        slug: space.slug,
        direction,
        jobId: job.jobId,
      });
      return { ok: true, space, job: serializeJob(job) };
    });

  return {
    app,
    close: async () => {
      await Promise.allSettled([agentStore.shutdown(), codexStore.shutdown()]);
    },
  };
}

async function checkStores(agentStore: AgentDataStore, codexStore: CodexSessionStore) {
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
    options.syncServerUrl ?? process.env.REMOTE_AUTONOMY_SYNC_SERVER_URL ?? DEFAULT_SYNC_SERVER_URL,
  );
  const connectStoresToSyncServer =
    options.connectStoresToSyncServer ?? truthy(process.env.REMOTE_AUTONOMY_CONNECT_SYNC ?? "1");
  return {
    agentDataPath:
      options.agentDataPath ??
      process.env.REMOTE_AUTONOMY_AGENT_DATA_PATH ??
      join(root, "agent-infra.db"),
    codexDataPath:
      options.codexDataPath ??
      process.env.REMOTE_AUTONOMY_CODEX_DATA_PATH ??
      join(root, "codex-sessions.db"),
    agentAppId: options.agentAppId ?? process.env.REMOTE_AUTONOMY_AGENT_APP_ID ?? "run-agent-infra",
    codexAppId: options.codexAppId ?? process.env.REMOTE_AUTONOMY_CODEX_APP_ID ?? "codex-sessions",
    syncServerUrl,
    syncServerAppId:
      options.syncServerAppId ??
      process.env.REMOTE_AUTONOMY_SYNC_SERVER_APP_ID ??
      DEFAULT_SYNC_SERVER_APP_ID,
    syncServerPathPrefix:
      options.syncServerPathPrefix ?? process.env.REMOTE_AUTONOMY_SYNC_SERVER_PATH_PREFIX,
    localSpacesRoot: stripTrailingSlash(
      options.localSpacesRoot ??
        process.env.REMOTE_AUTONOMY_LOCAL_SPACES_ROOT ??
        join(homedir(), ".designer", "spaces"),
    ),
    remoteSpacesRoot: stripTrailingSlash(
      options.remoteSpacesRoot ??
        process.env.REMOTE_AUTONOMY_REMOTE_SPACES_ROOT ??
        posix.join(DEFAULT_REMOTE_HOME, ".designer", "spaces"),
    ),
    objectStorageRegion:
      options.objectStorageRegion ??
      process.env.REMOTE_AUTONOMY_OBJECT_STORAGE_REGION ??
      DEFAULT_OBJECT_STORAGE_REGION,
    objectStorageBucket:
      options.objectStorageBucket ??
      process.env.REMOTE_AUTONOMY_OBJECT_STORAGE_BUCKET ??
      DEFAULT_OBJECT_STORAGE_BUCKET,
    designerSpacesPrefix: storageKey(
      options.designerSpacesPrefix ??
        process.env.REMOTE_AUTONOMY_DESIGNER_SPACES_PREFIX ??
        DEFAULT_DESIGNER_SPACES_PREFIX,
    ),
    backendSecret: options.backendSecret ?? process.env.REMOTE_AUTONOMY_BACKEND_SECRET,
    adminSecret: options.adminSecret ?? process.env.REMOTE_AUTONOMY_ADMIN_SECRET,
    hostId:
      options.hostId ?? process.env.REMOTE_AUTONOMY_HOST_ID ?? process.env.HOST ?? "unknown-host",
    env: options.env ?? process.env.REMOTE_AUTONOMY_ENV ?? "remote-autonomy",
    userBranch: options.userBranch ?? process.env.REMOTE_AUTONOMY_USER_BRANCH ?? "main",
    port:
      options.port ??
      (process.env.REMOTE_AUTONOMY_PORT ? Number(process.env.REMOTE_AUTONOMY_PORT) : 7474),
    connectStoresToSyncServer,
    syncServerProbeTimeoutMs:
      options.syncServerProbeTimeoutMs ??
      (process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS
        ? Number(process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS)
        : 3_000),
    syncServerProbe:
      options.syncServerProbe ??
      (() =>
        probeSyncServer(
          syncServerUrl,
          options.syncServerProbeTimeoutMs ??
            (process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS
              ? Number(process.env.REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS)
              : 3_000),
        )),
  };
}

type ResolvedOptions = ReturnType<typeof resolveOptions>;

async function listSpaceRecords(
  agentStore: AgentDataStore,
  limit: number,
): Promise<DesignerSpaceRecord[]> {
  const resultLimit = Math.max(0, Math.floor(limit));
  const jobs = await agentStore.listJobs({
    kind: SPACE_SYNC_JOB_KIND,
    includeFinished: true,
    limit: Math.max(resultLimit * 4, 50),
  });
  const spaces = new Map<string, DesignerSpaceRecord>();
  for (const job of jobs) {
    const space = spaceRecordFromJob(job);
    if (!space || spaces.has(space.slug)) {
      continue;
    }
    spaces.set(space.slug, space);
  }
  return [...spaces.values()].slice(0, resultLimit);
}

async function requireSpaceRecord(
  agentStore: AgentDataStore,
  slug: string,
): Promise<DesignerSpaceRecord> {
  const spaces = await listSpaceRecords(agentStore, 200);
  const space = spaces.find((candidate) => candidate.slug === slug);
  if (!space) {
    throw new GatewayError(404, `Designer space ${slug} not found`);
  }
  return space;
}

async function listSpaceFileRecords(
  agentStore: AgentDataStore,
  slug: string | undefined,
  limit: number,
  contentOptions?: {
    includeContent?: boolean;
    options: ResolvedOptions;
  },
): Promise<DesignerSpaceFileListRecord[]> {
  const resultLimit = Math.max(0, Math.floor(limit));
  if (resultLimit === 0) {
    return [];
  }
  const summary = await agentStore.getRunSummary(CONTROL_RUN_ID);
  const files = new Map<string, DesignerSpaceFileRecord>();
  for (const event of [...(summary?.semanticEvents ?? [])].reverse()) {
    if (event.event_type !== SPACE_FILE_EVENT_TYPE) {
      continue;
    }
    const file = spaceFileRecordFromEvent(event);
    if (!file || (slug && file.spaceSlug !== slug) || files.has(spaceFileKey(file))) {
      continue;
    }
    files.set(spaceFileKey(file), file);
    if (files.size >= resultLimit) {
      break;
    }
  }
  const records = [...files.values()];
  if (!contentOptions?.includeContent) {
    return records;
  }
  return await Promise.all(records.map(async (file) => {
    const bytes = await readCachedSpaceFileContent(contentOptions.options, file);
    return bytes ? { ...file, contentBase64: bytes.toString("base64") } : file;
  }));
}

function spaceFileRecordFromEvent(event: {
  payload_json?: unknown;
}): DesignerSpaceFileRecord | null {
  const payload = jsonObject(event.payload_json);
  const file = jsonObject(payload?.file);
  const objectStorage = jsonObject(file?.objectStorage);
  if (
    !file ||
    !objectStorage ||
    typeof file.objectRefId !== "string" ||
    typeof file.spaceSlug !== "string" ||
    typeof file.path !== "string" ||
    typeof file.localPath !== "string" ||
    typeof file.remotePath !== "string" ||
    typeof file.contentHash !== "string" ||
    typeof file.updatedAt !== "string" ||
    objectStorage.provider !== "oci" ||
    typeof objectStorage.region !== "string" ||
    typeof objectStorage.bucket !== "string" ||
    typeof objectStorage.prefix !== "string" ||
    typeof objectStorage.key !== "string" ||
    typeof objectStorage.uri !== "string"
  ) {
    return null;
  }
  return {
    objectRefId: file.objectRefId,
    spaceSlug: file.spaceSlug,
    path: file.path,
    localPath: file.localPath,
    remotePath: file.remotePath,
    contentHash: file.contentHash,
    sizeBytes: typeof file.sizeBytes === "number" ? file.sizeBytes : undefined,
    contentType: typeof file.contentType === "string" ? file.contentType : undefined,
    revisionId: typeof file.revisionId === "string" ? file.revisionId : undefined,
    writerId: typeof file.writerId === "string" ? file.writerId : undefined,
    sourceSession: typeof file.sourceSession === "string" ? file.sourceSession : undefined,
    uploadJobId: typeof file.uploadJobId === "string" ? file.uploadJobId : undefined,
    materializeJobId: typeof file.materializeJobId === "string" ? file.materializeJobId : undefined,
    materializeTarget:
      file.materializeTarget === "remote" || file.materializeTarget === "local"
        ? file.materializeTarget
        : "local",
    updatedAt: file.updatedAt,
    objectStorage: {
      provider: "oci",
      region: objectStorage.region,
      bucket: objectStorage.bucket,
      prefix: objectStorage.prefix,
      key: objectStorage.key,
      uri: objectStorage.uri,
    },
  };
}

function spaceRecordFromJob(job: JobRecord): DesignerSpaceRecord | null {
  const payload = jsonObject(job.payloadJson);
  const space = jsonObject(payload?.space);
  if (
    !space ||
    typeof space.slug !== "string" ||
    typeof space.title !== "string" ||
    typeof space.localPath !== "string" ||
    typeof space.remotePath !== "string"
  ) {
    return null;
  }
  const objectStorage = objectStorageFromSpacePayload(space);
  if (!objectStorage) {
    return null;
  }
  return {
    slug: space.slug,
    title: space.title,
    localPath: space.localPath,
    remotePath: space.remotePath,
    objectStoragePrefix: objectStorage.prefix,
    objectStorageUri: objectStorage.uri,
    objectStorage,
    syncKind: SPACE_SYNC_JOB_KIND,
  };
}

function resolveDesignerSpace(payload: JsonObject, options: ResolvedOptions): DesignerSpaceRecord {
  const slug = spaceSlug(requiredString(payload, "slug"));
  const title = optionalString(payload, "title") ?? slug;
  const localPath = stripTrailingSlash(
    optionalString(payload, "localPath") ?? join(options.localSpacesRoot, slug, SPACE_WORK_DIR),
  );
  const remotePath = stripTrailingSlash(
    optionalString(payload, "remotePath") ?? posix.join(options.remoteSpacesRoot, slug, SPACE_WORK_DIR),
  );
  const objectStoragePrefix = storageKey(options.designerSpacesPrefix, slug);
  const objectStorage = designerSpaceObjectStorage(options, objectStoragePrefix);
  return {
    slug,
    title,
    localPath,
    remotePath,
    objectStoragePrefix: objectStorage.prefix,
    objectStorageUri: objectStorage.uri,
    objectStorage,
    syncKind: SPACE_SYNC_JOB_KIND,
  };
}

function resolveDesignerSpaceFile(
  space: DesignerSpaceRecord,
  payload: JsonObject,
): DesignerSpaceFileRecord {
  const filePath = spaceFilePath(requiredString(payload, "path"));
  const contentHash = requiredString(payload, "contentHash");
  const objectKey = storageKey(space.objectStorage.prefix, "files", filePath);
  const materializeTarget = spaceFileMaterializeTarget(
    optionalString(payload, "materializeTarget"),
  );
  return {
    objectRefId: optionalString(payload, "objectRefId") ?? `space-file:${space.slug}:${filePath}`,
    spaceSlug: space.slug,
    path: filePath,
    localPath: join(space.localPath, ...filePath.split("/")),
    remotePath: posix.join(space.remotePath, filePath),
    contentHash,
    sizeBytes: optionalNumber(payload, "sizeBytes"),
    contentType: optionalString(payload, "contentType"),
    revisionId: optionalString(payload, "revisionId"),
    writerId: optionalString(payload, "writerId"),
    sourceSession: optionalString(payload, "sourceSession"),
    materializeTarget,
    updatedAt: new Date().toISOString(),
    objectStorage: {
      ...space.objectStorage,
      key: objectKey,
      uri: objectStorageObjectUri(
        space.objectStorage.region,
        space.objectStorage.bucket,
        objectKey,
      ),
    },
  };
}

function inlineSpaceFileContent(
  payload: JsonObject,
  file: DesignerSpaceFileRecord,
): Buffer | null {
  const contentBase64 = optionalString(payload, "contentBase64");
  const content = optionalString(payload, "content");
  if (!contentBase64 && !content) {
    return null;
  }
  const bytes = contentBase64 ? Buffer.from(contentBase64, "base64") : Buffer.from(content!, "utf8");
  const contentHash = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  if (contentHash !== file.contentHash) {
    throw new GatewayError(400, `contentHash mismatch for Designer space file ${file.path}`);
  }
  if (file.sizeBytes !== undefined && file.sizeBytes !== bytes.byteLength) {
    throw new GatewayError(400, `sizeBytes mismatch for Designer space file ${file.path}`);
  }
  return bytes;
}

async function writeInlineSpaceFile(
  options: ResolvedOptions,
  file: DesignerSpaceFileRecord,
  bytes: Buffer,
): Promise<InlineSpaceFileReceipt> {
  const objectCachePath = join(options.localSpacesRoot, OBJECT_CACHE_DIR, ...file.objectStorage.key.split("/"));
  const materializedPath = file.materializeTarget === "remote" ? file.remotePath : file.localPath;
  await Promise.all([
    writeFileWithParents(objectCachePath, bytes),
    writeFileWithParents(materializedPath, bytes),
  ]);
  return {
    objectCachePath,
    materializedPath,
    checksum: file.contentHash,
    bytes: bytes.byteLength,
    recordedAt: new Date().toISOString(),
  };
}

async function readCachedSpaceFileContent(
  options: ResolvedOptions,
  file: DesignerSpaceFileRecord,
): Promise<Buffer | null> {
  const objectCachePath = join(options.localSpacesRoot, OBJECT_CACHE_DIR, ...file.objectStorage.key.split("/"));
  const candidates = [
    objectCachePath,
    file.materializeTarget === "remote" ? file.remotePath : file.localPath,
    file.localPath,
    file.remotePath,
  ];
  for (const candidate of [...new Set(candidates)]) {
    try {
      const bytes = await readFile(candidate);
      const contentHash = `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
      if (contentHash === file.contentHash) {
        return bytes;
      }
    } catch {
      continue;
    }
  }
  return null;
}

async function writeFileWithParents(filePath: string, bytes: Buffer): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });
  await writeFile(filePath, bytes);
}

function designerSpaceObjectStorage(
  options: ResolvedOptions,
  prefix: string,
): ObjectStorageDescriptor {
  const region = options.objectStorageRegion;
  const bucket = options.objectStorageBucket;
  return {
    provider: "oci",
    region,
    bucket,
    prefix,
    uri: objectStorageUri(region, bucket, prefix),
  };
}

function objectStorageFromSpacePayload(space: JsonObject): ObjectStorageDescriptor | null {
  const objectStorage = jsonObject(space.objectStorage);
  if (
    objectStorage?.provider === "oci" &&
    typeof objectStorage.region === "string" &&
    typeof objectStorage.bucket === "string" &&
    typeof objectStorage.prefix === "string" &&
    typeof objectStorage.uri === "string"
  ) {
    const region = objectStorage.region.trim();
    const bucket = objectStorage.bucket.trim();
    const prefix = storageKey(objectStorage.prefix);
    if (!region || !bucket || !prefix) {
      return null;
    }
    const uri = objectStorageUri(region, bucket, prefix);
    if (objectStorage.uri !== uri) {
      return null;
    }
    return {
      provider: "oci",
      region,
      bucket,
      prefix,
      uri,
    };
  }
  if (typeof space.objectStoragePrefix !== "string" || typeof space.objectStorageUri !== "string") {
    return null;
  }
  return parseOciObjectStorage(space.objectStorageUri, space.objectStoragePrefix);
}

function objectStorageUri(region: string, bucket: string, prefix: string): string {
  return `oci://${region}/${bucket}/${prefix}/`;
}

function objectStorageObjectUri(region: string, bucket: string, key: string): string {
  return `oci://${region}/${bucket}/${key}`;
}

function parseOciObjectStorage(uri: string, prefix: string): ObjectStorageDescriptor | null {
  const match = /^oci:\/\/([^/]+)\/([^/]+)\/(.+)\/$/.exec(uri);
  if (!match) {
    return null;
  }
  const normalizedPrefix = storageKey(prefix);
  if (storageKey(match[3] ?? "") !== normalizedPrefix) {
    return null;
  }
  return {
    provider: "oci",
    region: match[1]!,
    bucket: match[2]!,
    prefix: normalizedPrefix,
    uri,
  };
}

function spaceJobDedupeKey(slug: string): string {
  return `${SPACE_SYNC_JOB_KIND}:${slug}`;
}

function spaceClaimId(slug: string): string {
  return `designer-space:${slug}`;
}

function spaceFileKey(file: Pick<DesignerSpaceFileRecord, "spaceSlug" | "path">): string {
  return `${file.spaceSlug}:${file.path}`;
}

function spaceFileEventId(file: DesignerSpaceFileRecord): string {
  return `${SPACE_FILE_EVENT_TYPE}:${file.spaceSlug}:${file.path}:${file.contentHash}`;
}

function spaceFileUploadDedupeKey(file: DesignerSpaceFileRecord): string {
  return `${SPACE_FILE_UPLOAD_JOB_KIND}:${file.spaceSlug}:${file.path}:${file.contentHash}`;
}

function spaceFileMaterializeDedupeKey(file: DesignerSpaceFileRecord): string {
  return `${SPACE_FILE_MATERIALIZE_JOB_KIND}:${file.spaceSlug}:${file.path}:${file.contentHash}:${file.materializeTarget}`;
}

function spaceSlug(value: string): string {
  const slug = value.trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(slug)) {
    throw new GatewayError(400, `invalid Designer space slug ${value}`);
  }
  return slug;
}

function spaceFilePath(value: string): string {
  const rawPath = value.trim();
  const normalized = posix.normalize(rawPath);
  if (
    rawPath === "" ||
    rawPath.includes("\\") ||
    normalized === "." ||
    normalized === ".." ||
    normalized.startsWith("../") ||
    normalized.startsWith("/") ||
    normalized.endsWith("/")
  ) {
    throw new GatewayError(400, `invalid Designer space file path ${value}`);
  }
  return normalized;
}

function syncDirection(value: string): "pull" | "push" {
  if (value === "pull" || value === "push") {
    return value;
  }
  throw new GatewayError(400, `invalid Designer space sync direction ${value}`);
}

function spaceFileMaterializeTarget(value: string | undefined): SpaceFileMaterializeTarget {
  if (value === undefined || value === "local") {
    return "local";
  }
  if (value === "remote") {
    return "remote";
  }
  throw new GatewayError(400, `invalid Designer space file materialize target ${value}`);
}

function storageKey(...segments: string[]): string {
  return segments
    .map((segment) => segment.trim().replace(/^\/+|\/+$/g, ""))
    .filter(Boolean)
    .join("/");
}

function jsonObject(value: unknown): JsonObject | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : null;
}

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

function storeConfig<T extends AgentDataStoreConfig | CodexSessionStoreConfig>(input: {
  dataPath: string;
  appId: string;
  options: ResolvedOptions;
}): T {
  mkdirSync(dirname(input.dataPath), { recursive: true });
  return {
    dataPath: input.dataPath,
    appId: input.appId,
    env: input.options.env,
    userBranch: input.options.userBranch,
    serverUrl: input.options.connectStoresToSyncServer ? input.options.syncServerUrl : undefined,
    serverPathPrefix: input.options.syncServerPathPrefix,
    backendSecret: input.options.backendSecret,
    adminSecret: input.options.adminSecret,
    tier: input.options.connectStoresToSyncServer ? "edge" : "local",
  } as T;
}

async function probeSyncServer(syncServerUrl: string, timeoutMs: number): Promise<SyncProbeResult> {
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

function serializeSemanticEvent(event: SemanticEvent) {
  return {
    eventId: event.event_id,
    runId: event.run_id,
    itemId: event.item_id ?? null,
    eventType: event.event_type,
    summaryText: event.summary_text ?? null,
    payloadJson: event.payload_json ?? null,
    occurredAt: event.occurred_at.toISOString(),
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

function optionalTimestamp(body: JsonObject, key: string): Date | string | number | undefined {
  const value = body[key];
  if (value === undefined || value === null || value === "") return undefined;
  if (value instanceof Date || typeof value === "string" || typeof value === "number") {
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

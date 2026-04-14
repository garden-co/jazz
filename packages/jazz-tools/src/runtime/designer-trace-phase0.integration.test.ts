import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  app as designerTraceApp,
  DESIGNER_TRACE_BRANCH_CONVENTION,
  DESIGNER_TRACE_RUNTIME_LAYOUT,
  DESIGNER_TRACE_SYNC_TARGET,
} from "./designer-trace-phase0.schema.js";
import { JazzClient } from "./client.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import {
  createDbFromClient,
  type Db,
  type QueryBuilder,
  type TableProxy,
} from "./db.js";
import { publishStoredSchema } from "./schema-fetch.js";

type TraceSessionRow = {
  id: string;
  session_id: string;
  schema_version: string;
  codebase_id: string;
  workspace_id: string;
  writer_id: string;
  install_id: string;
  writer_surface: string;
  started_at: Date;
  replication_scope: string;
  privacy_mode: string;
  hosted_sync_json: Record<string, unknown>;
  runtime_layout_json: Record<string, unknown>;
  ignore_contract_json: Record<string, unknown>;
  indexing_contract_json: Record<string, unknown>;
  benchmark_contract_json: Record<string, unknown>;
};

type TraceSessionInsert = Omit<TraceSessionRow, "id">;

type TraceEventRow = {
  id: string;
  event_id: string;
  session_id: string;
  session_row_id: string;
  schema_version: string;
  kind: string;
  occurred_at: Date;
  writer_id: string;
  replication_scope: string;
  privacy_mode: string;
  canonical_hash: string;
  code_state_id?: string;
  buffer_state_id?: string;
  checkpoint_id?: string;
  chunk_hash?: string;
  projection_id?: string;
  git_snapshot_id?: string;
  payload_json: Record<string, unknown>;
  refs_json: Record<string, unknown>;
};

type TraceEventInsert = Omit<TraceEventRow, "id">;

type StateHeadRow = {
  id: string;
  head_id: string;
  session_id: string;
  session_row_id: string;
  entity_kind: string;
  entity_key: string;
  current_state_id?: string;
  last_mutation_id?: string;
  tombstone: boolean;
  tombstoned_at?: Date;
  conflict_keys_json: string[];
  updated_at: Date;
};

type StateHeadInsert = Omit<StateHeadRow, "id">;

type StateMutationRow = {
  id: string;
  mutation_id: string;
  session_id: string;
  session_row_id: string;
  head_id: string;
  head_row_id: string;
  entity_kind: string;
  entity_key: string;
  mutation_kind: string;
  before_state_id?: string;
  after_state_id?: string;
  event_id?: string;
  checkpoint_id?: string;
  payload_json: Record<string, unknown>;
  occurred_at: Date;
};

type StateMutationInsert = Omit<StateMutationRow, "id">;

const TRACE_SCHEMA = designerTraceApp.wasmSchema;
const TRACE_SCHEMA_VERSION = "trace.designer.v1";
const require = createRequire(import.meta.url);
const NAPI_MODULE = require("jazz-napi") as {
  NapiRuntime: {
    new (
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      dataPath: string,
      tier?: string,
    ): {
      close?: () => Promise<void> | void;
    };
    inMemory(
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      tier?: string,
    ): {
      close?: () => Promise<void> | void;
    };
  };
  DevServer: {
    start(options: {
      appId: string;
      adminSecret: string;
      allowLocalFirstAuth: boolean;
    }): Promise<{
      appId: string;
      url: string;
      adminSecret?: string;
      stop(): Promise<void>;
    }>;
  };
  mintLocalFirstToken?: (seed: string, audience: string, ttlSeconds: number) => string;
};
const LOCAL_FIRST_AVAILABLE = typeof NAPI_MODULE.mintLocalFirstToken === "function";
const tempRoots: string[] = [];

const traceSessionsTable: TableProxy<TraceSessionRow, TraceSessionInsert> = {
  _table: "trace_sessions",
  _schema: TRACE_SCHEMA,
  _rowType: undefined as unknown as TraceSessionRow,
  _initType: undefined as unknown as TraceSessionInsert,
};

const traceEventsTable: TableProxy<TraceEventRow, TraceEventInsert> = {
  _table: "trace_events",
  _schema: TRACE_SCHEMA,
  _rowType: undefined as unknown as TraceEventRow,
  _initType: undefined as unknown as TraceEventInsert,
};

const stateHeadsTable: TableProxy<StateHeadRow, StateHeadInsert> = {
  _table: "state_heads",
  _schema: TRACE_SCHEMA,
  _rowType: undefined as unknown as StateHeadRow,
  _initType: undefined as unknown as StateHeadInsert,
};

const stateMutationsTable: TableProxy<StateMutationRow, StateMutationInsert> = {
  _table: "state_mutations",
  _schema: TRACE_SCHEMA,
  _rowType: undefined as unknown as StateMutationRow,
  _initType: undefined as unknown as StateMutationInsert,
};

beforeAll(async () => {
  await loadNapiModule();
});

afterEach(async () => {
  await Promise.all(tempRoots.splice(0).map((rootPath) => rm(rootPath, { recursive: true, force: true })));
});

async function createTempDataPath(prefix: string): Promise<string> {
  const rootPath = await mkdtemp(join(tmpdir(), prefix));
  tempRoots.push(rootPath);
  return join(rootPath, "runtime.db");
}

function makeQuery<T>(
  table: keyof typeof TRACE_SCHEMA,
  column?: string,
  value?: string,
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: TRACE_SCHEMA,
    _rowType: undefined as unknown as T,
    _build() {
      return JSON.stringify({
        table,
        conditions: column && value ? [{ column, op: "eq", value }] : [],
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

function makeLocalFirstSecret(): string {
  return randomBytes(32).toString("base64url");
}

async function mintLocalFirstJwt(seed: string, appId: string): Promise<string> {
  if (!NAPI_MODULE.mintLocalFirstToken) {
    throw new Error("jazz-napi local-first helpers are unavailable");
  }
  return NAPI_MODULE.mintLocalFirstToken(seed, appId, 3600);
}

async function createTraceNapiDb(options: {
  appId: string;
  jwtToken?: string;
  serverUrl?: string;
  dataPath?: string;
}): Promise<{
  db: Db;
  shutdown: () => Promise<void>;
}> {
  const schemaJson = JSON.stringify(TRACE_SCHEMA);
  const runtime = options.dataPath
    ? new NAPI_MODULE.NapiRuntime(
        schemaJson,
        options.appId,
        "test",
        DESIGNER_TRACE_BRANCH_CONVENTION.default_user_branch,
        options.dataPath,
        "edge",
      )
    : NAPI_MODULE.NapiRuntime.inMemory(
        schemaJson,
        options.appId,
        "test",
        DESIGNER_TRACE_BRANCH_CONVENTION.default_user_branch,
        "edge",
      );

  const client = JazzClient.connectWithRuntime(runtime as never, {
    appId: options.appId,
    schema: TRACE_SCHEMA,
    serverUrl: options.serverUrl,
    userBranch: DESIGNER_TRACE_BRANCH_CONVENTION.default_user_branch,
    jwtToken: options.jwtToken,
    tier: "edge",
    defaultDurabilityTier: "edge",
  });

  return {
    db: createDbFromClient(
      {
        appId: options.appId,
        driver: options.dataPath
          ? { type: "persistent", dbName: options.dataPath }
          : { type: "memory" },
        serverUrl: options.serverUrl,
        userBranch: DESIGNER_TRACE_BRANCH_CONVENTION.default_user_branch,
        jwtToken: options.jwtToken,
      },
      client,
    ),
    shutdown: async () => {
      await client.shutdown();
    },
  };
}

function makeTraceSession(scope: string, appId: string): TraceSessionInsert {
  return {
    session_id: `trace-session-${scope}-${randomUUID()}`,
    schema_version: TRACE_SCHEMA_VERSION,
    codebase_id: `codebase-${scope}`,
    workspace_id: `workspace-${scope}`,
    writer_id: `writer-${scope}`,
    install_id: `install-${scope}`,
    writer_surface: "designer.desktop",
    started_at: new Date(),
    replication_scope: "account_sync",
    privacy_mode: "private",
    hosted_sync_json: {
      ...DESIGNER_TRACE_SYNC_TARGET,
      app_id: appId,
    },
    runtime_layout_json: {
      version: "designer/indexer-v1",
      ...DESIGNER_TRACE_RUNTIME_LAYOUT,
    },
    ignore_contract_json: {
      sources: [".gitignore", ".ignore", ".rgignore", ".cursorignore"],
      hidden_entries_follow_search_rules: true,
      dedicated_designer_ignore_file: null,
    },
    indexing_contract_json: {
      lexical_sink: {
        sink_id: "designer.lexical-index.v1",
        reader_id: "designer.lexical-reader.v1",
        manifest: "workspaces/<workspace_id>/embeddable_files.txt",
        plaintext_local_only: true,
      },
      semantic_sink: {
        sink_id: "designer.helix-projection.v1",
        reader_id: "designer.semantic-reader.v1",
        projection_kind: "helix",
        plaintext_local_only: true,
        shared_reuse_requires_access_proof: true,
      },
      privacy: {
        default_replication_scope: "account_sync",
        default_privacy_mode: "private",
        shared_index_expectation: "obfuscated-or-encrypted",
        explicit_access_proof_required: true,
      },
    },
    benchmark_contract_json: {
      repo_size_classes: ["small", "medium", "large"],
      enforced_budgets: {
        store_mutation_emit_overhead_ms: 10,
        local_jazz_write_overhead_ms: 50,
      },
    },
  };
}

function makeTraceEvent(session: TraceSessionInsert, sessionRowId: string): TraceEventInsert {
  const eventId = `trace-event-${randomUUID()}`;
  return {
    event_id: eventId,
    session_id: session.session_id,
    session_row_id: sessionRowId,
    schema_version: TRACE_SCHEMA_VERSION,
    kind: "command.execute",
    occurred_at: new Date(),
    writer_id: session.writer_id,
    replication_scope: session.replication_scope,
    privacy_mode: session.privacy_mode,
    canonical_hash: `canonical-${randomUUID().replace(/-/g, "")}`,
    code_state_id: `code-state-${randomUUID()}`,
    buffer_state_id: `buffer-state-${randomUUID()}`,
    checkpoint_id: `checkpoint-${randomUUID()}`,
    chunk_hash: `chunk-${randomUUID().replace(/-/g, "")}`,
    projection_id: `projection-${randomUUID()}`,
    git_snapshot_id: `git-snapshot-${randomUUID()}`,
    payload_json: {
      kind: "command.execute",
      command_id: "workspace.saveAll",
      command_category: "workspace",
    },
    refs_json: {
      code_state_id: `code-state-${randomUUID()}`,
      buffer_state_id: `buffer-state-${randomUUID()}`,
      checkpoint_id: `checkpoint-${randomUUID()}`,
      chunk_hash: `chunk-${randomUUID().replace(/-/g, "")}`,
      projection_id: `projection-${randomUUID()}`,
      git_snapshot_id: `git-snapshot-${randomUUID()}`,
    },
  };
}

function makeStateHead(session: TraceSessionInsert, sessionRowId: string): StateHeadInsert {
  return {
    head_id: `state-head-${randomUUID()}`,
    session_id: session.session_id,
    session_row_id: sessionRowId,
    entity_kind: "checkpoint",
    entity_key: `checkpoint:${session.workspace_id}:src/lib/trace/types.ts`,
    current_state_id: `checkpoint-${randomUUID()}`,
    last_mutation_id: `state-mutation-${randomUUID()}`,
    tombstone: false,
    conflict_keys_json: [],
    updated_at: new Date(),
  };
}

function makeStateMutation(
  session: TraceSessionInsert,
  sessionRowId: string,
  head: StateHeadInsert,
  headRowId: string,
  eventId: string,
): StateMutationInsert {
  return {
    mutation_id: `state-mutation-${randomUUID()}`,
    session_id: session.session_id,
    session_row_id: sessionRowId,
    head_id: head.head_id,
    head_row_id: headRowId,
    entity_kind: head.entity_kind,
    entity_key: head.entity_key,
    mutation_kind: "checkpoint",
    after_state_id: head.current_state_id,
    event_id: eventId,
    checkpoint_id: head.current_state_id,
    payload_json: {
      relative_path: "src/lib/trace/types.ts",
      chunk_hashes: ["chunk-a", "chunk-b"],
    },
    occurred_at: new Date(),
  };
}

async function waitForQueryRows<T>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  timeoutMs = 20_000,
  options?: { tier?: "worker" | "edge" | "global" },
): Promise<T[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: T[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await db.all(query, options);
      if (predicate(rows)) {
        return rows;
      }
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 150));
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `timed out waiting for rows; lastRows=${JSON.stringify(lastRows)}, lastError=${lastErrorMessage}`,
  );
}

async function waitForSubscriptionRows<T extends { id: string }>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  timeoutMs = 20_000,
  options?: { tier?: "worker" | "edge" | "global" },
): Promise<T[]> {
  return await new Promise<T[]>((resolve, reject) => {
    let unsubscribe: () => void = () => {};
    const timeoutId = setTimeout(() => {
      unsubscribe();
      reject(new Error("timed out waiting for subscription rows"));
    }, timeoutMs);

    unsubscribe = db.subscribeAll(
      query,
      (delta) => {
        if (!predicate(delta.all)) {
          return;
        }
        clearTimeout(timeoutId);
        unsubscribe();
        resolve(delta.all);
      },
      options,
    );
  });
}

describe("designer trace phase 0 schema", () => {
  it("round-trips trace rows over local-first sync with subscribe/query coverage", async () => {
    const appId = randomUUID();
    const adminSecret = "designer-trace-phase0-admin-secret";
    let devServer: Awaited<ReturnType<typeof NAPI_MODULE.DevServer.start>> | null = null;
    let writerHandle: { db: Db; shutdown: () => Promise<void> } | null = null;
    let readerHandle: { db: Db; shutdown: () => Promise<void> } | null = null;

    try {
      devServer = await NAPI_MODULE.DevServer.start({
        appId,
        adminSecret,
        allowLocalFirstAuth: true,
      });
      const seed = makeLocalFirstSecret();
      const jwtToken = await mintLocalFirstJwt(seed, appId);
      await publishStoredSchema(devServer.url, {
        adminSecret,
        schema: TRACE_SCHEMA,
      });
      writerHandle = await createTraceNapiDb({
        appId,
        jwtToken,
        serverUrl: devServer.url,
      });
      readerHandle = await createTraceNapiDb({
        appId,
        jwtToken,
        serverUrl: devServer.url,
      });
      const writer = writerHandle.db;
      const reader = readerHandle.db;

      const session = makeTraceSession("local-sync", appId);
      const sessionRowsQuery = makeQuery<TraceSessionRow>(
        "trace_sessions",
        "session_id",
        session.session_id,
      );
      const eventRowsQuery = makeQuery<TraceEventRow>("trace_events", "session_id", session.session_id);
      const headRowsQuery = makeQuery<StateHeadRow>("state_heads", "session_id", session.session_id);
      const mutationRowsQuery = makeQuery<StateMutationRow>(
        "state_mutations",
        "session_id",
        session.session_id,
      );

      const subscriptionPromise = waitForSubscriptionRows(
        reader,
        eventRowsQuery,
        (rows) => rows.length > 0,
        30_000,
        { tier: "edge" },
      );

      const insertedSession = await writer.insertDurable(traceSessionsTable, session, { tier: "edge" });
      const head = makeStateHead(session, insertedSession.id);
      const insertedHead = await writer.insertDurable(stateHeadsTable, head, { tier: "edge" });
      const event = makeTraceEvent(session, insertedSession.id);
      await writer.insertDurable(traceEventsTable, event, { tier: "edge" });
      const mutation = makeStateMutation(
        session,
        insertedSession.id,
        head,
        insertedHead.id,
        event.event_id,
      );
      await writer.insertDurable(stateMutationsTable, mutation, { tier: "edge" });

      const subscribedEvents = await subscriptionPromise;
      expect(subscribedEvents.some((row) => row.event_id === event.event_id)).toBe(true);

      const [sessionRows, headRows, mutationRows] = await Promise.all([
        waitForQueryRows(reader, sessionRowsQuery, (rows) => rows.length === 1, 30_000, {
          tier: "edge",
        }),
        waitForQueryRows(reader, headRowsQuery, (rows) => rows.length === 1, 30_000, {
          tier: "edge",
        }),
        waitForQueryRows(reader, mutationRowsQuery, (rows) => rows.length === 1, 30_000, {
          tier: "edge",
        }),
      ]);

      expect(sessionRows[0]).toMatchObject({
        session_id: session.session_id,
        workspace_id: session.workspace_id,
        replication_scope: "account_sync",
        privacy_mode: "private",
      });
      expect(headRows[0]).toMatchObject({
        head_id: head.head_id,
        entity_kind: "checkpoint",
      });
      expect(mutationRows[0]).toMatchObject({
        mutation_id: mutation.mutation_id,
        event_id: event.event_id,
        mutation_kind: "checkpoint",
      });
    } finally {
      if (writerHandle) {
        await writerHandle.shutdown();
      }
      if (readerHandle) {
        await readerHandle.shutdown();
      }
      if (devServer) {
        await devServer.stop();
      }
    }
  }, 60_000);

  it("reopens persistent local-first databases without losing the canonical trace rows", async () => {
    const appId = randomUUID();
    const seed = makeLocalFirstSecret();
    const jwtToken = await mintLocalFirstJwt(seed, appId);
    const dataPath = await createTempDataPath("designer-trace-reopen-");
    const session = makeTraceSession("restart", appId);
    const sessionQuery = makeQuery<TraceSessionRow>("trace_sessions", "session_id", session.session_id);
    const eventQuery = makeQuery<TraceEventRow>("trace_events", "session_id", session.session_id);

    const firstHandle = await createTraceNapiDb({
      appId,
      jwtToken,
      dataPath,
    });
    const firstDb = firstHandle.db;

    try {
      const insertedSession = firstDb.insert(traceSessionsTable, session);
      firstDb.insert(traceEventsTable, makeTraceEvent(session, insertedSession.id));
    } finally {
      await firstHandle.shutdown();
    }

    const reopenedHandle = await createTraceNapiDb({
      appId,
      jwtToken,
      dataPath,
    });
    const reopenedDb = reopenedHandle.db;

    try {
      const reopenedSessions = await waitForQueryRows(
        reopenedDb,
        sessionQuery,
        (rows) => rows.length === 1,
        10_000,
      );
      const reopenedEvents = await waitForQueryRows(
        reopenedDb,
        eventQuery,
        (rows) => rows.length === 1,
        10_000,
      );

      expect(reopenedSessions[0]).toMatchObject({
        session_id: session.session_id,
        codebase_id: session.codebase_id,
      });
      expect(reopenedEvents[0]).toMatchObject({
        session_id: session.session_id,
        kind: "command.execute",
      });
    } finally {
      await reopenedHandle.shutdown();
    }
  }, 30_000);

  it.runIf(process.env.JAZZ_TRACE_ENABLE_HOSTED_SYNC === "1" && LOCAL_FIRST_AVAILABLE)(
    "smokes the hosted trace sync target with shared local-first identity material",
    async () => {
      const appId = DESIGNER_TRACE_SYNC_TARGET.app_id;
      const seed =
        process.env.JAZZ_TRACE_HOSTED_SYNC_SEED ?? "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
      const jwtToken = await mintLocalFirstJwt(seed, appId);
      const writerHandle = await createTraceNapiDb({
        appId,
        serverUrl: DESIGNER_TRACE_SYNC_TARGET.base_url,
        jwtToken,
      });
      const readerHandle = await createTraceNapiDb({
        appId,
        serverUrl: DESIGNER_TRACE_SYNC_TARGET.base_url,
        jwtToken,
      });
      const writer = writerHandle.db;
      const reader = readerHandle.db;

      try {
        const health = await fetch(`${DESIGNER_TRACE_SYNC_TARGET.base_url}/health`);
        expect(health.status).toBe(200);

        const session = makeTraceSession("hosted-sync", appId);
        const eventQuery = makeQuery<TraceEventRow>("trace_events", "session_id", session.session_id);
        const subscriptionPromise = waitForSubscriptionRows(
          reader,
          eventQuery,
          (rows) => rows.length > 0,
          60_000,
          { tier: "edge" },
        );

        const insertedSession = await writer.insertDurable(traceSessionsTable, session, {
          tier: "edge",
        });
        const event = makeTraceEvent(session, insertedSession.id);
        await writer.insertDurable(traceEventsTable, event, { tier: "edge" });

        const subscribedRows = await subscriptionPromise;
        expect(subscribedRows.some((row) => row.event_id === event.event_id)).toBe(true);
      } finally {
        await writerHandle.shutdown();
        await readerHandle.shutdown();
      }
    },
    90_000,
  );
});

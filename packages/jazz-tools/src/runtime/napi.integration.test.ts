import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Row } from "./client.js";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
import { translateQuery } from "./query-adapter.js";
import { loadCompiledSchema } from "../schema-loader.js";
import { pushSchemaCatalogue, startLocalJazzServer } from "../testing/local-jazz-server.js";
import { createNapiRuntime, loadNapiModule } from "./testing/napi-runtime-test-utils.js";

type SimpleTodo = {
  id: string;
  title: string;
  done: boolean;
};

type SimpleTodoInit = {
  title: string;
  done: boolean;
};

type TimestampProject = {
  id: string;
  name: string;
  created_at: Date;
  updated_at: Date;
};

type TimestampProjectInit = {
  name: string;
  created_at: Date;
  updated_at: Date;
};

type PolicyTodo = {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  parentId?: string;
  projectId?: string;
  owner_id: string;
};

type PolicyTodoInit = {
  title: string;
  done: boolean;
  description?: string;
  parentId?: string;
  projectId?: string;
  owner_id: string;
};

type SyncRequestBody = {
  client_id: string;
  payloads: unknown[];
};

type ObjectMutationRequest = {
  method: string;
  pathname: string;
  headers: IncomingMessage["headers"];
  body: Record<string, unknown>;
};

type SyncCaptureServerHandle = {
  baseUrl: string;
  eventClientIds: string[];
  syncRequests: Array<{
    headers: IncomingMessage["headers"];
    body: SyncRequestBody;
  }>;
  objectRequests: ObjectMutationRequest[];
  closeLatestStream(): void;
  stop(): Promise<void>;
};

const JWT_KID = "napi-test-kid";
const JWT_SECRET = "napi-test-secret";

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const TIMESTAMP_SCHEMA: WasmSchema = {
  projects: {
    columns: [
      { name: "name", column_type: { type: "Text" }, nullable: false },
      { name: "created_at", column_type: { type: "Timestamp" }, nullable: false },
      { name: "updated_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
};

let todoServerWasmSchemaPromise: Promise<WasmSchema> | null = null;

async function loadTodoServerWasmSchema(): Promise<WasmSchema> {
  if (!todoServerWasmSchemaPromise) {
    todoServerWasmSchemaPromise = loadCompiledSchema(TODO_SERVER_SCHEMA_DIR).then(
      (compiled) => compiled.wasmSchema,
    );
  }
  return await todoServerWasmSchemaPromise;
}

const simpleTodosTable: TableProxy<SimpleTodo, SimpleTodoInit> = {
  _table: "todos",
  _schema: TEST_SCHEMA,
  _rowType: undefined as unknown as SimpleTodo,
  _initType: undefined as unknown as SimpleTodoInit,
};

const allTodosQuery: QueryBuilder<SimpleTodo> = {
  _table: "todos",
  _schema: TEST_SCHEMA,
  _rowType: undefined as unknown as SimpleTodo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
      offset: 0,
    });
  },
};

const timestampProjectsTable: TableProxy<TimestampProject, TimestampProjectInit> = {
  _table: "projects",
  _schema: TIMESTAMP_SCHEMA,
  _rowType: undefined as unknown as TimestampProject,
  _initType: undefined as unknown as TimestampProjectInit,
};

function makePolicyTodosTable(schema: WasmSchema): TableProxy<PolicyTodo, PolicyTodoInit> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _initType: undefined as unknown as PolicyTodoInit,
  };
}

function makeAllPolicyTodosQuery(schema: WasmSchema): QueryBuilder<PolicyTodo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

function makePolicyTodoByIdQuery(schema: WasmSchema, id: string): QueryBuilder<PolicyTodo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "id", op: "eq", value: id }],
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

const BASIC_SCHEMA_DIR = fileURLToPath(new URL("../testing/fixtures/basic", import.meta.url));
const TODO_SERVER_SCHEMA_DIR = fileURLToPath(
  new URL("../../../../examples/todo-server-ts", import.meta.url),
);

beforeAll(async () => {
  await loadNapiModule();
});

function encodeFrames(events: unknown[]): Uint8Array {
  const encoder = new TextEncoder();
  const chunks = events.map((event) => {
    const payload = encoder.encode(JSON.stringify(event));
    const frame = new Uint8Array(4 + payload.length);
    new DataView(frame.buffer).setUint32(0, payload.length, false);
    frame.set(payload, 4);
    return frame;
  });

  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

async function readRequestBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function getAvailablePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createNetServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close((error) => {
          if (error) reject(error);
          else reject(new Error("failed to allocate an available port"));
        });
        return;
      }

      server.close((error) => {
        if (error) reject(error);
        else resolve(address.port);
      });
    });
  });
}

async function listen(server: HttpServer): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("failed to determine listening address"));
        return;
      }
      resolve(address.port);
    });
  });
}

async function startSyncCaptureServer(): Promise<SyncCaptureServerHandle> {
  const syncRequests: SyncCaptureServerHandle["syncRequests"] = [];
  const objectRequests: SyncCaptureServerHandle["objectRequests"] = [];
  const eventClientIds: string[] = [];
  const openStreams = new Set<ServerResponse>();
  const server = createHttpServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");

    if (request.method === "GET" && url.pathname === "/events") {
      const clientId = `server-client-${eventClientIds.length + 1}`;
      eventClientIds.push(clientId);
      openStreams.add(response);
      response.once("close", () => {
        openStreams.delete(response);
      });
      response.writeHead(200, { "Content-Type": "application/octet-stream" });
      response.write(encodeFrames([{ type: "Connected", client_id: clientId }]));
      return;
    }

    if (request.method === "POST" && url.pathname === "/sync") {
      const rawBody = await readRequestBody(request);
      syncRequests.push({
        headers: request.headers,
        body: JSON.parse(rawBody) as SyncRequestBody,
      });
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end("{}");
      return;
    }

    if (
      (request.method === "POST" || request.method === "PUT") &&
      (url.pathname === "/sync/object" || url.pathname === "/sync/object/delete")
    ) {
      const rawBody = await readRequestBody(request);
      objectRequests.push({
        method: request.method,
        pathname: url.pathname,
        headers: request.headers,
        body: JSON.parse(rawBody) as Record<string, unknown>,
      });

      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(
        JSON.stringify(
          request.method === "POST" && url.pathname === "/sync/object"
            ? { object_id: `captured-object-${objectRequests.length}` }
            : {},
        ),
      );
      return;
    }

    if (request.method === "GET" && url.pathname === "/health") {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ status: "ok" }));
      return;
    }

    response.writeHead(404);
    response.end("not found");
  });

  const port = await listen(server);

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    eventClientIds,
    syncRequests,
    objectRequests,
    closeLatestStream() {
      const latest = Array.from(openStreams).at(-1);
      latest?.destroy();
    },
    async stop() {
      for (const stream of openStreams) {
        stream.destroy();
      }
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
  };
}

async function waitForQueryRows<T>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  timeoutMs = 20_000,
  queryOptions: { tier?: "worker" | "edge" | "global" } = { tier: "edge" },
): Promise<T[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: T[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await db.all(query, queryOptions);
      if (predicate(rows)) return rows;
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

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function formatDiagnostics(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function snapshotDbClientState(db: Db): Record<string, unknown> {
  const client = (db as any).runtimeClient;
  const streamController = client?.streamController;

  return {
    serverClientId: client?.serverClientId ?? null,
    useBackendSyncAuth: client?.useBackendSyncAuth ?? null,
    resolvedSessionUserId: client?.resolvedSession?.user_id ?? null,
    stream: streamController
      ? {
          streamAttached: streamController.streamAttached ?? null,
          streamConnecting: streamController.streamConnecting ?? null,
          reconnectAttempt: streamController.reconnectAttempt ?? null,
          hasReconnectTimer: Boolean(streamController.reconnectTimer),
          hasAbortController: Boolean(streamController.streamAbortController),
          activeServerUrl: streamController.activeServerUrl ?? null,
          activeServerPathPrefix: streamController.activeServerPathPrefix ?? null,
          stopped: streamController.stopped ?? null,
        }
      : null,
  };
}

async function fetchHealthSnapshot(serverUrl: string): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 1_000);
  try {
    const response = await fetch(`${serverUrl}/health`, {
      signal: controller.signal,
    });
    return {
      ok: response.ok,
      status: response.status,
    };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    clearTimeout(timeoutId);
  }
}

async function withTimeoutDiagnostics<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
  diagnostics: () => Promise<Record<string, unknown>>,
): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(async () => {
          const snapshot = await diagnostics().catch((error) => ({
            diagnosticsError: error instanceof Error ? error.message : String(error),
          }));
          reject(new Error(`${label} after ${timeoutMs}ms\n${formatDiagnostics(snapshot)}`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function base64Url(input: Buffer | string): string {
  const encoded = (input instanceof Buffer ? input : Buffer.from(input)).toString("base64");
  return encoded.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

class JwksServer {
  private readonly server: HttpServer;
  readonly url: string;

  private constructor(server: HttpServer, url: string) {
    this.server = server;
    this.url = url;
  }

  static async start(secret: string): Promise<JwksServer> {
    const server = createHttpServer((request, response) => {
      if (request.url !== "/jwks") {
        response.statusCode = 404;
        response.end("not found");
        return;
      }

      response.statusCode = 200;
      response.setHeader("Content-Type", "application/json");
      response.end(
        JSON.stringify({
          keys: [
            {
              kty: "oct",
              kid: JWT_KID,
              alg: "HS256",
              k: base64Url(secret),
            },
          ],
        }),
      );
    });

    const port = await getAvailablePort();
    await new Promise<void>((resolve, reject) => {
      server.listen(port, "127.0.0.1", (error?: unknown) => {
        if (error) reject(error);
        else resolve();
      });
    });

    return new JwksServer(server, `http://127.0.0.1:${port}/jwks`);
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

function isNestedOutboxCall(call: unknown[]): call is [null, [string, string, string, boolean]] {
  return (
    call[0] === null &&
    Array.isArray(call[1]) &&
    typeof call[1][0] === "string" &&
    typeof call[1][1] === "string" &&
    typeof call[1][2] === "string" &&
    typeof call[1][3] === "boolean"
  );
}

function isQuerySubscriptionPayload(payloadJson: string): boolean {
  try {
    const payload = JSON.parse(payloadJson) as Record<string, unknown>;
    return "QuerySubscription" in payload;
  } catch {
    return false;
  }
}

function hasPayloadKind(payloadJson: string, payloadKind: string): boolean {
  try {
    const payload = JSON.parse(payloadJson) as Record<string, unknown>;
    return payloadKind in payload;
  } catch {
    return false;
  }
}

function isQuerySubscriptionRequest(request: { body: SyncRequestBody }): boolean {
  return request.body.payloads.some(
    (p) =>
      typeof p === "object" && p !== null && "QuerySubscription" in (p as Record<string, unknown>),
  );
}

async function settleAsyncSyncWork(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

function toBase64Url(value: unknown): string {
  return base64Url(Buffer.from(JSON.stringify(value), "utf8"));
}

function makeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.signature`;
}

function buildClientQuerySubscriptionPayload(queryJson: string, queryId = 1): string {
  return JSON.stringify({
    QuerySubscription: {
      query_id: queryId,
      query: JSON.parse(queryJson) as Record<string, unknown>,
      session: null,
      propagation: "full",
    },
  });
}

async function createTempDir(prefix: string): Promise<string> {
  return await mkdtemp(join(tmpdir(), prefix));
}

describe("NAPI integration", () => {
  it("supports oversized indexed persistent mutations from JS callers", async () => {
    const { NapiRuntime } = await loadNapiModule();
    const dataDir = await createTempDir("jazz-napi-large-index-");
    const dataPath = join(dataDir, "jazz.db");
    const runtime = new NapiRuntime(
      serializeRuntimeSchema(TEST_SCHEMA),
      `napi-large-index-${randomUUID()}`,
      "test",
      "main",
      dataPath,
    ) as unknown as {
      insert(table: string, values: unknown): Row;
      update(objectId: string, updates: Record<string, unknown>): void;
      query(queryJson: string): Promise<Row[]>;
      close(): void;
    };

    const oversizedTitle = "x".repeat(40_000);
    const updatedOversizedTitle = "y".repeat(45_000);
    const queryJson = translateQuery(allTodosQuery._build(), TEST_SCHEMA);

    try {
      const insertedRow = runtime.insert("todos", {
        title: { type: "Text", value: oversizedTitle },
        done: { type: "Boolean", value: false },
      });

      let rows = await runtime.query(queryJson);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ id: insertedRow.id });
      expect(rows[0]?.values[0]).toEqual({ type: "Text", value: oversizedTitle });
      expect(rows[0]?.values[1]).toEqual({ type: "Boolean", value: false });

      const secondRow = runtime.insert("todos", {
        title: { type: "Text", value: "kept title" },
        done: { type: "Boolean", value: false },
      });

      runtime.update(secondRow.id, {
        title: { type: "Text", value: updatedOversizedTitle },
      });

      rows = await runtime.query(queryJson);
      expect(rows).toHaveLength(2);

      const insertedOversized = rows.find((row) => row.id === insertedRow.id);
      expect(insertedOversized).toBeDefined();
      expect(insertedOversized?.values[0]).toEqual({ type: "Text", value: oversizedTitle });
      expect(insertedOversized?.values[1]).toEqual({ type: "Boolean", value: false });

      const updatedOversized = rows.find((row) => row.id === secondRow.id);
      expect(updatedOversized).toBeDefined();
      expect(updatedOversized?.values[0]).toEqual({
        type: "Text",
        value: updatedOversizedTitle,
      });
      expect(updatedOversized?.values[1]).toEqual({ type: "Boolean", value: false });
    } finally {
      runtime.close();
      await rm(dataDir, { recursive: true, force: true });
    }
  }, 20_000);

  it("emits the real nested onSyncMessageToSend callback shape from the compiled addon", async () => {
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `napi-contract-${randomUUID()}`,
      tier: "worker",
    });
    const queryJson = translateQuery(allTodosQuery._build(), TEST_SCHEMA);
    const rawCalls: unknown[][] = [];

    runtime.addServer();
    runtime.onSyncMessageToSend((...args: unknown[]) => {
      rawCalls.push(args);
    });

    const handle = runtime.subscribe(queryJson, () => undefined, undefined, "edge", undefined);

    await vi.waitFor(
      () => {
        expect(
          rawCalls.some(
            (call) => isNestedOutboxCall(call) && isQuerySubscriptionPayload(call[1][2]),
          ),
        ).toBe(true);
      },
      { timeout: 15_000 },
    );

    runtime.unsubscribe(handle);

    expect(rawCalls.every((call) => isNestedOutboxCall(call))).toBe(true);

    const querySubscriptionCall = rawCalls.find(
      (call) => isNestedOutboxCall(call) && isQuerySubscriptionPayload(call[1][2]),
    );

    expect(querySubscriptionCall).toBeDefined();
    expect(querySubscriptionCall?.[1]).toEqual([
      "server",
      expect.any(String),
      expect.any(String),
      false,
    ]);
  }, 20_000);

  it("routes client-originated subscriptions back through the real nested client callback shape", async () => {
    const runtime = await createNapiRuntime(TEST_SCHEMA, {
      appId: `napi-client-contract-${randomUUID()}`,
      tier: "edge",
    });
    const queryJson = translateQuery(allTodosQuery._build(), TEST_SCHEMA);
    const rawCalls: unknown[][] = [];

    runtime.onSyncMessageToSend((...args: unknown[]) => {
      rawCalls.push(args);
    });

    const clientId = runtime.addClient();
    runtime.setClientRole?.(clientId, "peer");
    runtime.onSyncMessageReceivedFromClient?.(
      clientId,
      buildClientQuerySubscriptionPayload(queryJson),
    );

    await vi.waitFor(
      () => {
        expect(
          rawCalls.some(
            (call) =>
              isNestedOutboxCall(call) &&
              call[1][0] === "client" &&
              call[1][1] === clientId &&
              hasPayloadKind(call[1][2], "QuerySettled"),
          ),
        ).toBe(true);
      },
      { timeout: 15_000 },
    );

    const insertedRow = runtime.insert("todos", {
      title: { type: "Text", value: "client-synced-item" },
      done: { type: "Boolean", value: false },
    });

    await vi.waitFor(
      () => {
        expect(
          rawCalls.some(
            (call) =>
              isNestedOutboxCall(call) &&
              call[1][0] === "client" &&
              call[1][1] === clientId &&
              hasPayloadKind(call[1][2], "RowVersionNeeded"),
          ),
        ).toBe(true);
      },
      { timeout: 15_000 },
    );

    expect(rawCalls.every((call) => isNestedOutboxCall(call))).toBe(true);
    expect(insertedRow.id).toEqual(expect.any(String));
  }, 20_000);

  it("posts backend query subscriptions upstream via createJazzContext(...).asBackend()", async () => {
    const captureServer = await startSyncCaptureServer();
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: `napi-backend-sync-${randomUUID()}`,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: captureServer.baseUrl,
        backendSecret: "napi-backend-secret",
      });

      const db = context.asBackend();
      const unsubscribe = db.subscribeAll(allTodosQuery, () => undefined, { tier: "edge" });

      await vi.waitFor(
        () => expect(captureServer.syncRequests.filter(isQuerySubscriptionRequest)).toHaveLength(1),
        {
          timeout: 15_000,
        },
      );

      unsubscribe();

      const request = captureServer.syncRequests.find(isQuerySubscriptionRequest);
      if (!request) {
        throw new Error("expected a QuerySubscription sync request");
      }
      expect(request.headers["x-jazz-backend-secret"]).toBe("napi-backend-secret");
      expect(request.headers.authorization).toBeUndefined();
      expect(request.headers["x-jazz-local-mode"]).toBeUndefined();
      expect(request.headers["x-jazz-local-token"]).toBeUndefined();
      expect(request.body.client_id).toBe("server-client-1");
      expect(
        request.body.payloads.find(
          (p) =>
            typeof p === "object" &&
            p !== null &&
            "QuerySubscription" in (p as Record<string, unknown>),
        ),
      ).toBeDefined();
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await captureServer.stop();
    }
  }, 20_000);

  it("replays active backend query subscriptions after the events stream reconnects", async () => {
    const captureServer = await startSyncCaptureServer();
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: `napi-backend-reconnect-${randomUUID()}`,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: captureServer.baseUrl,
        backendSecret: "napi-backend-secret",
      });

      const db = context.asBackend();
      const unsubscribe = db.subscribeAll(allTodosQuery, () => undefined, { tier: "edge" });

      await vi.waitFor(
        () => expect(captureServer.syncRequests.filter(isQuerySubscriptionRequest)).toHaveLength(1),
        {
          timeout: 15_000,
        },
      );
      expect(captureServer.eventClientIds).toEqual(["server-client-1"]);

      captureServer.closeLatestStream();

      await vi.waitFor(() => expect(captureServer.eventClientIds).toHaveLength(2), {
        timeout: 15_000,
      });
      await vi.waitFor(
        () => expect(captureServer.syncRequests.filter(isQuerySubscriptionRequest)).toHaveLength(2),
        {
          timeout: 15_000,
        },
      );

      unsubscribe();

      const querySubscriptions = captureServer.syncRequests.filter(isQuerySubscriptionRequest);
      expect(querySubscriptions[1]?.body.client_id).toBe("server-client-2");
      expect(querySubscriptions[1]?.headers["x-jazz-backend-secret"]).toBe("napi-backend-secret");
    } finally {
      consoleError.mockRestore();
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await captureServer.stop();
    }
  }, 25_000);

  it("applies createJazzContext(...).forSession() mutations through high-level Db APIs", async () => {
    const port = await getAvailablePort();
    const appId = randomUUID();
    const backendSecret = "napi-session-secret";
    const adminSecret = "napi-session-admin-secret";
    const server = await startLocalJazzServer({
      appId,
      port,
      backendSecret,
      adminSecret,
    });
    let context: {
      asBackend(): Db;
      forSession(session: { user_id: string; claims: Record<string, unknown> }): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
        env: "test",
        userBranch: "main",
      });
      const todoServerSchema = await loadTodoServerWasmSchema();
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const aliceDb = context.forSession({
        user_id: "alice",
        claims: { role: "editor", team: "alpha" },
      });

      const createdTodo = await withTimeout(
        aliceDb.insertDurable(
          policyTodosTable,
          {
            title: "session-created-item",
            done: false,
            description: "created via forSession",
            owner_id: "alice",
          },
          { tier: "edge" },
        ),
        10_000,
        "session insert timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend session read timed out",
            ),
          ).toMatchObject({
            id: createdTodo.id,
            title: "session-created-item",
            done: false,
            owner_id: "alice",
          });
        },
        { timeout: 20_000 },
      );

      await expect(
        aliceDb.insertDurable(
          policyTodosTable,
          {
            title: "session-policy-denied",
            done: false,
            description: "",
            owner_id: "bob",
          },
          { tier: "edge" },
        ),
      ).rejects.toThrow();

      await withTimeout(
        aliceDb.updateDurable(policyTodosTable, createdTodo.id, { done: true }, { tier: "edge" }),
        10_000,
        "session update timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend session update read timed out",
            ),
          ).toMatchObject({
            id: createdTodo.id,
            done: true,
          });
        },
        { timeout: 20_000 },
      );

      await withTimeout(
        aliceDb.deleteDurable(policyTodosTable, createdTodo.id, { tier: "edge" }),
        10_000,
        "session delete timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend session delete read timed out",
            ),
          ).toBeNull();
        },
        { timeout: 20_000 },
      );
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await server.stop();
    }
  }, 60_000);

  it("extracts JWT request auth and applies createJazzContext(...).forRequest() mutations via Db", async () => {
    const port = await getAvailablePort();
    const appId = randomUUID();
    const backendSecret = "napi-request-secret";
    const adminSecret = "napi-request-admin-secret";
    const server = await startLocalJazzServer({
      appId,
      port,
      backendSecret,
      adminSecret,
    });
    let context: {
      asBackend(): Db;
      forRequest(request: { headers: Record<string, string> }): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
        env: "test",
        userBranch: "main",
      });
      const todoServerSchema = await loadTodoServerWasmSchema();
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);
      const allPolicyTodosQuery = makeAllPolicyTodosQuery(todoServerSchema);

      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });

      const backendDb = context.asBackend();
      const requestDb = context.forRequest({
        headers: {
          authorization: `Bearer ${makeJwt({
            sub: "request-user",
            claims: { role: "reviewer", tenant: "beta" },
          })}`,
        },
      });

      const createdTodo = await withTimeout(
        requestDb.insertDurable(
          policyTodosTable,
          {
            title: "request-created-item",
            done: false,
            description: "created via forRequest",
            owner_id: "request-user",
          },
          { tier: "edge" },
        ),
        10_000,
        "request insert timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              requestDb.all(allPolicyTodosQuery, { tier: "edge" }),
              10_000,
              "request-scoped read timed out",
            ),
          ).toEqual([
            expect.objectContaining({
              id: createdTodo.id,
              title: "request-created-item",
              owner_id: "request-user",
            }),
          ]);
        },
        { timeout: 20_000 },
      );

      await expect(
        requestDb.insertDurable(
          policyTodosTable,
          {
            title: "request-policy-denied",
            done: false,
            description: "",
            owner_id: "someone-else",
          },
          { tier: "edge" },
        ),
      ).rejects.toThrow();

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend request read timed out",
            ),
          ).toMatchObject({
            id: createdTodo.id,
            title: "request-created-item",
            owner_id: "request-user",
          });
        },
        { timeout: 20_000 },
      );
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await server.stop();
    }
  }, 60_000);

  it("filters session-scoped query reads over backend-authenticated sync", async () => {
    const port = await getAvailablePort();
    const appId = randomUUID();
    const backendSecret = "napi-query-backend-secret";
    const adminSecret = "napi-query-admin-secret";
    const rowTitles = (rows: PolicyTodo[]): string[] => rows.map((row) => row.title).sort();

    const jwks = await JwksServer.start(JWT_SECRET);
    const server = await startLocalJazzServer({
      appId,
      port,
      jwksUrl: jwks.url,
      backendSecret,
      adminSecret,
    });
    let writerContext: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    let readerContext: {
      asBackend(): Db;
      forSession(session: { user_id: string; claims: Record<string, unknown> }): Db;
      forRequest(request: { headers: Record<string, string> }): Db;
      shutdown(): Promise<void>;
    } | null = null;
    const operationTimeoutMs = 20_000;
    const timeline: string[] = [];
    const mark = (label: string) => {
      timeline.push(`${Date.now()}: ${label}`);
    };

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
        env: "test",
        userBranch: "main",
      });
      mark("schema catalogue pushed");
      const todoServerSchema = await loadTodoServerWasmSchema();
      mark("server schema loaded");
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);
      const allPolicyTodosQuery = makeAllPolicyTodosQuery(todoServerSchema);

      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });
      mark("writer contexts created");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });

      const writerBackend = writerContext.asBackend();
      const readerBackend = readerContext.asBackend();
      const collectDiagnostics = async () => ({
        timeline,
        serverUrl: server.url,
        serverHealth: await fetchHealthSnapshot(server.url),
        writerBackend: {
          state: snapshotDbClientState(writerBackend),
          queryRows: await readerBackend
            .all(allPolicyTodosQuery, { tier: "worker" })
            .then((rows) => rowTitles(rows))
            .catch((error) => [
              `reader-worker-query-failed:${
                error instanceof Error ? error.message : String(error)
              }`,
            ]),
        },
        readerBackend: snapshotDbClientState(readerBackend),
      });
      mark("db handles acquired");

      // Warm the lazy NAPI contexts via real edge reads so the assertions below
      // measure session-scoped sync visibility instead of first-use startup time.
      await Promise.all([
        waitForQueryRows(writerBackend, allPolicyTodosQuery, (rows) => rows.length === 0),
        waitForQueryRows(readerBackend, allPolicyTodosQuery, (rows) => rows.length === 0),
      ]);
      mark("warm edge reads resolved");

      // Seed through a separate backend-authenticated writer runtime so the
      // read assertions below still exercise sync, while avoiding unrelated
      // flakiness from three independent client-authenticated edge acks.
      mark("starting bob durable insert");
      await withTimeoutDiagnostics(
        writerBackend.insertDurable(
          policyTodosTable,
          {
            title: "bob-item",
            done: false,
            description: "",
            owner_id: "bob",
          },
          { tier: "edge" },
        ),
        operationTimeoutMs,
        "bob writer create timed out",
        collectDiagnostics,
      );
      mark("bob durable insert resolved");
      mark("starting carol durable insert");
      await withTimeoutDiagnostics(
        writerBackend.insertDurable(
          policyTodosTable,
          {
            title: "carol-item",
            done: false,
            description: "",
            owner_id: "carol",
          },
          { tier: "edge" },
        ),
        operationTimeoutMs,
        "carol writer create timed out",
        collectDiagnostics,
      );
      mark("carol durable insert resolved");
      mark("starting alice durable insert");
      await withTimeoutDiagnostics(
        writerBackend.insertDurable(
          policyTodosTable,
          {
            title: "alice-item",
            done: false,
            description: "",
            owner_id: "alice",
          },
          { tier: "edge" },
        ),
        operationTimeoutMs,
        "alice writer create timed out",
        collectDiagnostics,
      );
      mark("alice durable insert resolved");

      const aliceSessionDb = readerContext.forSession({
        user_id: "alice",
        claims: {},
      });
      const aliceRequestDb = readerContext.forRequest({
        headers: {
          authorization: `Bearer ${makeJwt({ sub: "alice" })}`,
        },
      });

      await vi.waitFor(
        async () => {
          expect(
            rowTitles(
              await withTimeoutDiagnostics(
                readerBackend.all(allPolicyTodosQuery, { tier: "edge" }),
                operationTimeoutMs,
                "backend reader query timed out",
                collectDiagnostics,
              ),
            ),
          ).toEqual(["alice-item", "bob-item", "carol-item"]);
        },
        { timeout: 20_000 },
      );

      await vi.waitFor(
        async () => {
          expect(
            rowTitles(
              await withTimeoutDiagnostics(
                aliceSessionDb.all(allPolicyTodosQuery, { tier: "edge" }),
                operationTimeoutMs,
                "alice session query timed out",
                collectDiagnostics,
              ),
            ),
          ).toEqual(["alice-item"]);
        },
        { timeout: 20_000 },
      );

      await vi.waitFor(
        async () => {
          expect(
            rowTitles(
              await withTimeoutDiagnostics(
                aliceRequestDb.all(allPolicyTodosQuery, { tier: "edge" }),
                operationTimeoutMs,
                "alice request query timed out",
                collectDiagnostics,
              ),
            ),
          ).toEqual(["alice-item"]);
        },
        { timeout: 20_000 },
      );
    } finally {
      if (writerContext) {
        await writerContext.shutdown();
      }
      if (readerContext) {
        await readerContext.shutdown();
      }
      await settleAsyncSyncWork();
      await server.stop();
      await jwks.stop();
    }
  }, 60_000);

  it("syncs edge create/update/delete flows between real backend NAPI contexts", async () => {
    const port = await getAvailablePort();
    const appId = randomUUID();
    const backendSecret = "napi-e2e-backend-secret";
    const adminSecret = "napi-e2e-admin-secret";
    const server = await startLocalJazzServer({
      appId,
      port,
      backendSecret,
      adminSecret,
    });
    let writerContext: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    let readerContext: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: BASIC_SCHEMA_DIR,
      });

      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
      });
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
      });
      await settleAsyncSyncWork();

      const writer = writerContext.asBackend();
      const reader = readerContext.asBackend();

      await waitForQueryRows(reader, allTodosQuery, (rows) => rows.length === 0);

      const createdRow = await writer.insertDurable(
        simpleTodosTable,
        {
          title: "napi-shared-item",
          done: false,
        },
        { tier: "edge" },
      );
      const rowId = createdRow.id;

      const rowsAfterCreate = await waitForQueryRows(reader, allTodosQuery, (rows) =>
        rows.some((row) => row.id === rowId),
      );
      const replicatedRow = rowsAfterCreate.find((row) => row.id === rowId);
      expect(replicatedRow).toMatchObject({
        id: rowId,
        title: "napi-shared-item",
        done: false,
      });

      await writer.updateDurable(simpleTodosTable, rowId, { done: true }, { tier: "edge" });

      const rowsAfterUpdate = await waitForQueryRows(reader, allTodosQuery, (rows) => {
        const row = rows.find((entry) => entry.id === rowId);
        return row?.done === true;
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.done).toBe(true);

      await writer.deleteDurable(simpleTodosTable, rowId, { tier: "edge" });
      await settleAsyncSyncWork();
      await waitForQueryRows(
        writer,
        allTodosQuery,
        (rows) => !rows.some((row) => row.id === rowId),
      );
      await readerContext.shutdown();
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
      });
      await settleAsyncSyncWork();
      const refreshedReader = readerContext.asBackend();
      await waitForQueryRows(
        refreshedReader,
        allTodosQuery,
        (rows) => !rows.some((row) => row.id === rowId),
      );
    } finally {
      if (writerContext) {
        await writerContext.shutdown();
      }
      if (readerContext) {
        await readerContext.shutdown();
      }
      await settleAsyncSyncWork();
      await server.stop();
    }
  }, 60_000);

  it("reopens persistent backend runtimes cleanly and retains local data", async () => {
    const dataRoot = await createTempDir("jazz-napi-persistent-");
    const dataPath = join(dataRoot, "runtime.db");
    const appId = randomUUID();
    let writerContext: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    let reopenedContext: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      const writer = writerContext.db();
      const createdRow = await writer.insertDurable(
        simpleTodosTable,
        {
          title: "persisted-local-item",
          done: false,
        },
        { tier: "worker" },
      );
      const rowId = createdRow.id;

      await waitForQueryRows(
        writer,
        allTodosQuery,
        (rows) => rows.some((row) => row.id === rowId),
        10_000,
        { tier: "worker" },
      );

      await writerContext.shutdown();
      writerContext = null;
      await settleAsyncSyncWork();

      reopenedContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      const reopened = reopenedContext.db();
      const reopenedRows = await waitForQueryRows(
        reopened,
        allTodosQuery,
        (rows) => rows.some((row) => row.id === rowId),
        10_000,
        { tier: "worker" },
      );

      const reopenedRow = reopenedRows.find((row) => row.id === rowId);
      expect(reopenedRow).toMatchObject({
        id: rowId,
        title: "persisted-local-item",
        done: false,
      });
    } finally {
      if (writerContext) {
        await writerContext.shutdown();
      }
      if (reopenedContext) {
        await reopenedContext.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("accepts modern epoch-millisecond timestamps from the TS value converter on backend durable writes", async () => {
    const dataRoot = await createTempDir("jazz-napi-timestamp-");
    const dataPath = join(dataRoot, "runtime.db");
    const timestamp = 1773285322816;
    let context: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      context = createJazzContext({
        appId: randomUUID(),
        app: { wasmSchema: TIMESTAMP_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      await expect(
        context.db().insertDurable(
          timestampProjectsTable,
          {
            name: "timestamp-probe",
            created_at: new Date(timestamp),
            updated_at: new Date(timestamp),
          },
          { tier: "worker" },
        ),
      ).resolves.toEqual({
        id: expect.any(String),
        name: "timestamp-probe",
        created_at: new Date(timestamp),
        updated_at: new Date(timestamp),
      });
    } finally {
      if (context) {
        await context.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);
});

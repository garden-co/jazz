import { createHash, createHmac, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type Server as HttpServer,
} from "node:http";
import { createServer as createNetServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Row } from "./client.js";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
import { translateQuery } from "./query-adapter.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
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

type WsAuthConfig = {
  jwt_token?: string | null;
  backend_secret?: string | null;
  admin_secret?: string | null;
};

type SyncCaptureServerHandle = {
  baseUrl: string;
  /** Sequential client IDs sent to each connecting client in the Connected frame. */
  eventClientIds: string[];
  /** One entry per SyncBatchRequest received over WebSocket. */
  syncRequests: Array<{
    /** Auth from the client's AuthHandshake (replaces HTTP headers). */
    auth: WsAuthConfig;
    body: SyncRequestBody;
  }>;
  /** Drop the most-recently-opened WebSocket connection. */
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

let todoServerProjectPromise: Promise<LoadedSchemaProject> | null = null;

async function loadTodoServerProject(): Promise<LoadedSchemaProject> {
  if (!todoServerProjectPromise) {
    todoServerProjectPromise = loadCompiledSchema(TODO_SERVER_SCHEMA_DIR);
  }
  return await todoServerProjectPromise;
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

function makePolicyTodosByDescriptionQuery(
  schema: WasmSchema,
  description: string,
): QueryBuilder<PolicyTodo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "description", op: "eq", value: description }],
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

// ---------------------------------------------------------------------------
// Minimal WebSocket server helpers (no external deps — pure node:http/crypto)
// ---------------------------------------------------------------------------

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function wsHandshake(req: IncomingMessage, socket: Socket): void {
  const key = req.headers["sec-websocket-key"] as string;
  const accept = createHash("sha1")
    .update(key + WS_GUID)
    .digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
  );
}

function sendWsBinaryFrame(socket: Socket, payload: Buffer): void {
  const len = payload.length;
  let header: Buffer;
  if (len <= 125) {
    header = Buffer.allocUnsafe(2);
    header[0] = 0x82; // FIN + binary
    header[1] = len;
  } else if (len <= 65535) {
    header = Buffer.allocUnsafe(4);
    header[0] = 0x82;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.allocUnsafe(10);
    header[0] = 0x82;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  socket.write(Buffer.concat([header, payload]));
}

function sendWsJson(socket: Socket, json: unknown): void {
  const inner = Buffer.from(JSON.stringify(json), "utf8");
  const outer = Buffer.allocUnsafe(4 + inner.length);
  outer.writeUInt32BE(inner.length, 0);
  inner.copy(outer, 4);
  sendWsBinaryFrame(socket, outer);
}

/**
 * Parse as many complete WebSocket binary frames as possible from `buf`.
 * Returns the decoded payloads and any unconsumed bytes.
 */
function parseWsFrames(buf: Buffer): { payloads: Buffer[]; remaining: Buffer } {
  const payloads: Buffer[] = [];
  let pos = 0;

  while (pos + 2 <= buf.length) {
    const opcode = buf[pos] & 0x0f;
    const masked = (buf[pos + 1] & 0x80) !== 0;
    let payloadLen = buf[pos + 1] & 0x7f;
    let headerEnd = pos + 2;

    if (payloadLen === 126) {
      if (pos + 4 > buf.length) break;
      payloadLen = buf.readUInt16BE(pos + 2);
      headerEnd = pos + 4;
    } else if (payloadLen === 127) {
      if (pos + 10 > buf.length) break;
      payloadLen = Number(buf.readBigUInt64BE(pos + 2));
      headerEnd = pos + 10;
    }

    const maskEnd = headerEnd + (masked ? 4 : 0);
    const frameEnd = maskEnd + payloadLen;
    if (frameEnd > buf.length) break;

    if (opcode === 0x2 /* binary */ || opcode === 0x1 /* text */) {
      const payload = Buffer.allocUnsafe(payloadLen);
      if (masked) {
        const mask = buf.slice(headerEnd, headerEnd + 4);
        for (let i = 0; i < payloadLen; i++) {
          payload[i] = buf[maskEnd + i] ^ mask[i % 4];
        }
      } else {
        buf.copy(payload, 0, maskEnd, frameEnd);
      }
      payloads.push(payload);
    } else if (opcode === 0x8 /* close */) {
      // Acknowledge with a close frame and stop parsing.
      socket_close_ack: {
        break socket_close_ack;
      }
      break;
    }

    pos = frameEnd;
  }

  return { payloads, remaining: buf.slice(pos) };
}

async function startSyncCaptureServer(): Promise<SyncCaptureServerHandle> {
  const syncRequests: SyncCaptureServerHandle["syncRequests"] = [];
  const eventClientIds: string[] = [];
  const openSockets = new Set<Socket>();

  const server = createHttpServer((_req, res) => {
    res.writeHead(404);
    res.end("not found");
  });

  server.on("upgrade", (req: IncomingMessage, socket: Socket) => {
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }

    // Complete the WebSocket handshake.
    wsHandshake(req, socket);
    openSockets.add(socket);
    socket.once("close", () => openSockets.delete(socket));
    socket.once("error", () => openSockets.delete(socket));

    // Each WS connection gets one auth (from the first Jazz frame).
    let connAuth: WsAuthConfig = {};
    let receivedHandshake = false;
    let pendingBuf: Buffer = Buffer.alloc(0);

    socket.on("data", (chunk: Buffer) => {
      pendingBuf = Buffer.concat([pendingBuf, chunk]);
      const { payloads, remaining } = parseWsFrames(pendingBuf);
      pendingBuf = remaining;

      for (const payload of payloads) {
        // Each Jazz payload is a length-prefixed JSON frame.
        if (payload.length < 4) continue;
        const innerLen = payload.readUInt32BE(0);
        if (payload.length < 4 + innerLen) continue;
        const msg = JSON.parse(payload.slice(4, 4 + innerLen).toString("utf8")) as Record<
          string,
          unknown
        >;

        if (!receivedHandshake) {
          // First frame is the AuthHandshake.
          receivedHandshake = true;
          connAuth = (msg.auth ?? {}) as WsAuthConfig;
          const clientId = `server-client-${eventClientIds.length + 1}`;
          eventClientIds.push(clientId);
          sendWsJson(socket, {
            type: "Connected",
            connection_id: eventClientIds.length,
            client_id: clientId,
            next_sync_seq: 0,
            catalogue_state_hash: null,
          });
        } else {
          // Subsequent frames are SyncBatchRequests.
          syncRequests.push({
            auth: connAuth,
            body: msg as SyncRequestBody,
          });
        }
      }
    });
  });

  const port = await listen(server);

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    eventClientIds,
    syncRequests,
    closeLatestStream() {
      const latest = Array.from(openSockets).at(-1);
      latest?.destroy();
    },
    async stop() {
      for (const sock of openSockets) sock.destroy();
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
  const header = toBase64Url({ alg: "HS256", typ: "JWT", kid: JWT_KID });
  const body = toBase64Url(payload);
  const signature = createHmac("sha256", JWT_SECRET)
    .update(`${header}.${body}`, "utf8")
    .digest("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
  return `${header}.${body}.${signature}`;
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

type TempRuntimeData = {
  dataRoot: string;
  dataPath: string;
};

async function createTempRuntimeData(prefix: string): Promise<TempRuntimeData> {
  const dataRoot = await createTempDir(prefix);
  return {
    dataRoot,
    dataPath: join(dataRoot, "runtime.db"),
  };
}

async function cleanupTempRuntimeData(data: TempRuntimeData | null): Promise<void> {
  if (!data) {
    return;
  }
  await rm(data.dataRoot, { recursive: true, force: true });
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
    let runtimeData: TempRuntimeData | null = null;
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      runtimeData = await createTempRuntimeData("jazz-napi-backend-sync-");
      context = createJazzContext({
        appId: `napi-backend-sync-${randomUUID()}`,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
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
      expect(request.auth.backend_secret).toBe("napi-backend-secret");
      expect(request.auth.jwt_token).toBeFalsy();
      expect(request.body.client_id).toEqual(expect.any(String));
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
      await cleanupTempRuntimeData(runtimeData);
      await captureServer.stop();
    }
  }, 20_000);

  // The WebSocket transport uses a single handshake auth per connection.
  // In backend mode, the NAPI path sends backend_secret on the sync socket;
  // admin_secret is NOT included (it was only used per-HTTP-request for
  // catalogue publishing in the old transport). Catalogue publishing via a
  // separate admin path is planned but not yet implemented.
  it("sends catalogue payloads over the backend-auth WebSocket connection", async () => {
    const captureServer = await startSyncCaptureServer();
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: `napi-backend-catalogue-${randomUUID()}`,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "memory" },
        serverUrl: captureServer.baseUrl,
        backendSecret: "napi-backend-secret",
        adminSecret: "napi-admin-secret",
      });

      const db = context.asBackend();
      const unsubscribe = db.subscribeAll(allTodosQuery, () => undefined, { tier: "edge" });

      await vi.waitFor(
        () =>
          expect(
            captureServer.syncRequests.some((request) =>
              request.body.payloads.some(
                (payload) =>
                  typeof payload === "object" &&
                  payload !== null &&
                  "CatalogueEntryUpdated" in (payload as Record<string, unknown>),
              ),
            ),
          ).toBe(true),
        {
          timeout: 15_000,
        },
      );

      const request = captureServer.syncRequests.find((candidate) =>
        candidate.body.payloads.some(
          (payload) =>
            typeof payload === "object" &&
            payload !== null &&
            "CatalogueEntryUpdated" in (payload as Record<string, unknown>),
        ),
      );
      if (!request) {
        throw new Error("expected a CatalogueEntryUpdated sync request");
      }

      // In backend mode, the WS handshake carries backend_secret.
      // admin_secret is not sent on the sync socket (it will use a
      // separate catalogue-publish endpoint once that's implemented).
      expect(request.auth.backend_secret).toBe("napi-backend-secret");
      expect(request.auth.admin_secret ?? null).toBeNull();
      expect(request.auth.jwt_token ?? null).toBeNull();
      unsubscribe();
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
    let runtimeData: TempRuntimeData | null = null;
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      runtimeData = await createTempRuntimeData("jazz-napi-backend-reconnect-");
      context = createJazzContext({
        appId: `napi-backend-reconnect-${randomUUID()}`,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
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
      expect(querySubscriptions[1]?.body.client_id).toEqual(expect.any(String));
      expect(querySubscriptions[1]?.auth.backend_secret).toBe("napi-backend-secret");
    } finally {
      consoleError.mockRestore();
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await captureServer.stop();
    }
  }, 25_000);

  it("applies createJazzContext(...).forSession() mutations through high-level Db APIs", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-session-secret";
    const adminSecret = "napi-session-admin-secret";
    let runtimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
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
      const todoServerProject = await loadTodoServerProject();
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      runtimeData = await createTempRuntimeData("jazz-napi-session-runtime-");
      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
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
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
    }
  }, 60_000);

  it("extracts JWT request auth and applies createJazzContext(...).forRequest() mutations via Db", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-request-secret";
    const adminSecret = "napi-request-admin-secret";
    const scopeTag = `request-scope-${randomUUID()}`;
    let runtimeData: TempRuntimeData | null = null;
    const jwks = await JwksServer.start(JWT_SECRET);
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });
    let context: {
      asBackend(): Db;
      forRequest(request: { headers: Record<string, string> }): Promise<Db>;
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
      const todoServerProject = await loadTodoServerProject();
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);
      const scopedPolicyTodosQuery = makePolicyTodosByDescriptionQuery(todoServerSchema, scopeTag);

      runtimeData = await createTempRuntimeData("jazz-napi-request-runtime-");
      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        jwksUrl: jwks.url,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });

      const backendDb = context.asBackend();
      const requestDb = await context.forRequest({
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
            description: scopeTag,
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
              requestDb.all(scopedPolicyTodosQuery, { tier: "edge" }),
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
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
      await jwks.stop();
    }
  }, 60_000);

  it("filters session-scoped query reads over backend-authenticated sync", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-query-backend-secret";
    const adminSecret = "napi-query-admin-secret";
    const rowTitles = (rows: PolicyTodo[]): string[] => rows.map((row) => row.title).sort();
    const scopeTag = `session-scope-${randomUUID()}`;
    let writerRuntimeData: TempRuntimeData | null = null;
    let readerRuntimeData: TempRuntimeData | null = null;

    const jwks = await JwksServer.start(JWT_SECRET);
    const server = await startLocalJazzServer({
      appId,
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
      forRequest(request: { headers: Record<string, string> }): Promise<Db>;
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
      const todoServerProject = await loadTodoServerProject();
      const todoServerSchema = todoServerProject.wasmSchema;
      mark("server schema loaded");
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);
      const scopedPolicyTodosQuery = makePolicyTodosByDescriptionQuery(todoServerSchema, scopeTag);

      writerRuntimeData = await createTempRuntimeData("jazz-napi-query-writer-");
      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: writerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        jwksUrl: jwks.url,
        env: "test",
        userBranch: "main",
        tier: "worker",
      });
      mark("writer contexts created");
      readerRuntimeData = await createTempRuntimeData("jazz-napi-query-reader-");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        jwksUrl: jwks.url,
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
            .all(scopedPolicyTodosQuery, { tier: "worker" })
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
        waitForQueryRows(writerBackend, scopedPolicyTodosQuery, (rows) => rows.length === 0),
        waitForQueryRows(readerBackend, scopedPolicyTodosQuery, (rows) => rows.length === 0),
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
            description: scopeTag,
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
            description: scopeTag,
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
            description: scopeTag,
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
      const aliceRequestDb = await readerContext.forRequest({
        headers: {
          authorization: `Bearer ${makeJwt({ sub: "alice" })}`,
        },
      });

      await vi.waitFor(
        async () => {
          expect(
            rowTitles(
              await withTimeoutDiagnostics(
                readerBackend.all(scopedPolicyTodosQuery, { tier: "edge" }),
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
                aliceSessionDb.all(scopedPolicyTodosQuery, { tier: "edge" }),
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
                aliceRequestDb.all(scopedPolicyTodosQuery, { tier: "edge" }),
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
      await cleanupTempRuntimeData(writerRuntimeData);
      await cleanupTempRuntimeData(readerRuntimeData);
      await server.stop();
      await jwks.stop();
    }
  }, 60_000);

  it("syncs edge create/update/delete flows between real backend NAPI contexts", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-e2e-backend-secret";
    const adminSecret = "napi-e2e-admin-secret";
    let writerRuntimeData: TempRuntimeData | null = null;
    let readerRuntimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
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

      writerRuntimeData = await createTempRuntimeData("jazz-napi-sync-writer-");
      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: writerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
      });
      readerRuntimeData = await createTempRuntimeData("jazz-napi-sync-reader-");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
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
      await cleanupTempRuntimeData(readerRuntimeData);
      readerRuntimeData = await createTempRuntimeData("jazz-napi-sync-reader-reopen-");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
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
      await cleanupTempRuntimeData(writerRuntimeData);
      await cleanupTempRuntimeData(readerRuntimeData);
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

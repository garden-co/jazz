import { randomUUID } from "node:crypto";
import {
  createServer as createHttpServer,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from "node:http";
import { createServer as createNetServer } from "node:net";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient, Row } from "./client.js";
import type { QueryBuilder } from "./db.js";
import { translateQuery } from "./query-adapter.js";
import { pushSchemaCatalogue, startLocalJazzServer } from "../testing/local-jazz-server.js";
import { createNapiRuntime, loadNapiModule } from "./testing/napi-runtime-test-utils.js";

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

type SyncRequestBody = {
  client_id: string;
  payload: unknown;
};

type SyncCaptureServerHandle = {
  baseUrl: string;
  syncRequests: Array<{
    headers: IncomingMessage["headers"];
    body: SyncRequestBody;
  }>;
  stop(): Promise<void>;
};

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const allTodosQuery: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: TEST_SCHEMA,
  _rowType: undefined as unknown as Todo,
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

const BASIC_SCHEMA_DIR = fileURLToPath(new URL("../testing/fixtures/basic", import.meta.url));

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
  const openStreams = new Set<ServerResponse>();
  const server = createHttpServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");

    if (request.method === "GET" && url.pathname === "/events") {
      openStreams.add(response);
      response.once("close", () => {
        openStreams.delete(response);
      });
      response.writeHead(200, { "Content-Type": "application/octet-stream" });
      response.write(encodeFrames([{ type: "Connected", client_id: "server-client-1" }]));
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
    syncRequests,
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

async function waitForRows(
  client: JazzClient,
  predicate: (rows: Row[]) => boolean,
  timeoutMs = 20_000,
): Promise<Row[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: Row[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await client.query(allTodosQuery, { tier: "edge" });
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

async function settleAsyncSyncWork(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

describe("NAPI integration", () => {
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

  it("posts backend query subscriptions upstream via createJazzContext(...).asBackend()", async () => {
    const captureServer = await startSyncCaptureServer();
    let context: {
      asBackend(): JazzClient;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: `napi-backend-sync-${randomUUID()}`,
        app: { wasmSchema: TEST_SCHEMA },
        driver: { type: "memory" },
        serverUrl: captureServer.baseUrl,
        backendSecret: "napi-backend-secret",
      });

      const client = context.asBackend();
      const subscriptionId = client.subscribe(allTodosQuery, () => undefined, { tier: "edge" });

      await vi.waitFor(() => expect(captureServer.syncRequests).toHaveLength(1), {
        timeout: 15_000,
      });

      client.unsubscribe(subscriptionId);

      const request = captureServer.syncRequests[0];
      expect(request.headers["x-jazz-backend-secret"]).toBe("napi-backend-secret");
      expect(request.headers.authorization).toBeUndefined();
      expect(request.headers["x-jazz-local-mode"]).toBeUndefined();
      expect(request.headers["x-jazz-local-token"]).toBeUndefined();
      expect(request.body.client_id).toBe("server-client-1");
      expect(request.body.payload).toHaveProperty("QuerySubscription");
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await captureServer.stop();
    }
  }, 20_000);

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
      asBackend(): JazzClient;
      shutdown(): Promise<void>;
    } | null = null;
    let readerContext: {
      asBackend(): JazzClient;
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
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
      });
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
      });

      const writer = writerContext.asBackend();
      const reader = readerContext.asBackend();

      await waitForRows(reader, (rows) => rows.length === 0);

      const rowId = await writer.create(
        "todos",
        [
          { type: "Text", value: "napi-shared-item" },
          { type: "Boolean", value: false },
        ],
        { tier: "edge" },
      );

      const rowsAfterCreate = await waitForRows(reader, (rows) =>
        rows.some((row) => row.id === rowId),
      );
      const createdRow = rowsAfterCreate.find((row) => row.id === rowId);
      expect(createdRow?.values[0]).toEqual({ type: "Text", value: "napi-shared-item" });
      expect(createdRow?.values[1]).toEqual({ type: "Boolean", value: false });

      await writer.update(rowId, { done: { type: "Boolean", value: true } }, { tier: "edge" });

      const rowsAfterUpdate = await waitForRows(reader, (rows) => {
        const row = rows.find((entry) => entry.id === rowId);
        return Boolean(row?.values[1]?.type === "Boolean" && row.values[1].value === true);
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.values[1]).toEqual({ type: "Boolean", value: true });

      await writer.delete(rowId, { tier: "edge" });
      await waitForRows(reader, (rows) => !rows.some((row) => row.id === rowId));
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
});

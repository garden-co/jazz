import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocket } from "undici";
import type { WasmSchema } from "../../drivers/types.js";
import { fetchSchemaHashes, fetchStoredWasmSchema, publishStoredSchema } from "../schema-fetch.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../../testing/index.js";
import { JazzClient } from "../client.js";
import { createWasmRuntime, hasJazzWasmBuild } from "../testing/wasm-runtime-test-utils.js";
import {
  createNapiNativeRuntimeAdapter,
  hasJazzNapiBuild,
} from "../testing/napi-runtime-test-utils.js";
import { encodeSchema } from "./native-runtime-adapter.js";
import { decodeNativeDelta, isNativeRowDelta } from "../subscription-manager.js";
import type { SubscriptionWireDelta } from "../../drivers/types.js";

const maybeIt = hasJazzWasmBuild() ? it : it.skip;
const maybeNapiIt = hasJazzNapiBuild() ? it : it.skip;
const previousWebSocket = globalThis.WebSocket;

const schema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
} satisfies WasmSchema;

function normalizeTestDelta(delta: SubscriptionWireDelta, testSchema: WasmSchema) {
  if (isNativeRowDelta(delta)) {
    const columns =
      testSchema.todos?.columns ?? testSchema.arrays?.columns ?? testSchema.values?.columns;
    if (!columns) throw new Error("test schema has no decodable subscription table");
    return decodeNativeDelta(delta, columns);
  }
  return delta;
}

const writableTodoSchema = {
  todos: {
    ...schema.todos,
    policies: {
      select: { using: { type: "True" } },
      insert: { with_check: { type: "True" } },
      update: { using: { type: "True" }, with_check: { type: "True" } },
      delete: { using: { type: "True" } },
    },
  },
} satisfies WasmSchema;

const arraySchema = {
  arrays: {
    columns: [{ name: "data", column_type: { type: "Bytea" }, nullable: false }],
  },
} satisfies WasmSchema;

const largeValueSchema = {
  values: {
    columns: [
      { name: "kind", column_type: { type: "Text" }, nullable: false },
      { name: "text", column_type: { type: "Text" }, nullable: true },
      { name: "bytes", column_type: { type: "Bytea" }, nullable: true },
    ],
  },
} satisfies WasmSchema;

describe("NativeRuntimeAdapter server convergence", () => {
  let server: LocalJazzServerHandle | null = null;
  const clients: JazzClient[] = [];
  const tempRoots: string[] = [];

  afterEach(async () => {
    await Promise.allSettled(clients.splice(0).map((client) => client.shutdown()));
    await server?.stop();
    server = null;
    await Promise.allSettled(
      tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })),
    );
    globalThis.WebSocket = previousWebSocket;
  });

  it("syncs writes between two JazzClient connections through /apps/:app/ws", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const appId = "00000000-0000-0000-0000-00000000c001";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "native-runtime-convergence-admin",
      schema: encodeSchema(schema),
    });

    const clientA = await createClient({ appId, serverUrl: server.url, peer: "alice" });
    const clientB = await createClient({ appId, serverUrl: server.url, peer: "bob" });
    clients.push(clientA, clientB);

    clientA.connectTransport(server.url, { admin_secret: server.adminSecret });
    clientB.connectTransport(server.url, { admin_secret: server.adminSecret });

    const observedBySubscription = new Promise<string>((resolve) => {
      clientB.subscribe(
        JSON.stringify({ table: "todos" }),
        (delta) => {
          for (const change of normalizeTestDelta(delta, schema)) {
            const firstValue = "row" in change ? change.row?.values[0] : undefined;
            if (firstValue?.type === "Text") {
              resolve(firstValue.value);
            }
          }
        },
        { tier: "local" },
      );
    });

    const inserted = clientA.insert("todos", {
      title: { type: "Text", value: "websocket convergence" },
      done: { type: "Boolean", value: false },
    });

    await waitForPromise(inserted.wait({ tier: "edge" }), "client A insert did not settle at edge");
    await waitForPromise(
      observedBySubscription,
      "client B subscription did not observe the native runtime insert",
    );

    const convergedRows = await waitFor(async () => {
      const rows = await clientB.query(JSON.stringify({ table: "todos" }), { tier: "local" });
      return rows.find((row) => row.id === inserted.value.id);
    });

    expect(convergedRows).toMatchObject({
      id: inserted.value.id,
      values: [
        { type: "Text", value: "websocket convergence" },
        { type: "Boolean", value: false },
      ],
    });
  }, 15_000);

  maybeIt(
    "persists websocket writes across server restart",
    async () => {
      globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

      const tempRoot = await mkdtemp(join(tmpdir(), "jazz-native-runtime-restart-"));
      tempRoots.push(tempRoot);
      const dataDir = join(tempRoot, "server-data");
      const appId = "00000000-0000-0000-0000-00000000c002";
      const adminSecret = "native-runtime-restart-admin";

      server = await startLocalJazzServer({
        appId,
        dataDir,
        adminSecret,
        schema: encodeSchema(schema),
      });
      const published = await publishSchema(server);

      const immediateWriter = await createClient({
        appId,
        serverUrl: server.url,
        peer: "immediate-writer",
      });
      clients.push(immediateWriter);
      immediateWriter.connectTransport(server.url, { admin_secret: server.adminSecret });

      const immediateInsert = immediateWriter.insert("todos", {
        title: { type: "Text", value: "websocket durable restart" },
        done: { type: "Boolean", value: false },
      });
      await waitForPromise(
        immediateInsert.wait({ tier: "edge" }),
        "writer insert did not settle at edge after dynamic schema publish",
      );

      await immediateWriter.shutdown();
      clients.splice(clients.indexOf(immediateWriter), 1);

      const wrongSecretResponse = await fetch(`${server.url}/apps/${appId}/admin/schemas`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": "not-the-admin-secret",
        },
        body: JSON.stringify({ schema: { tables: schema } }),
      });
      expect(wrongSecretResponse.status).toBe(401);

      const port = server.port;
      await server.stop();
      server = null;

      server = await startLocalJazzServer({
        appId,
        port,
        dataDir,
        adminSecret,
      });

      const catalogue = await fetchSchemaHashes(server.url, { appId, adminSecret });
      expect(catalogue.hashes).toContain(published.hash);

      const storedSchema = await fetchStoredWasmSchema(server.url, {
        appId,
        adminSecret,
        schemaHash: published.hash,
      });
      expect(storedSchema.schema).toEqual(schema);

      const writer = await createClient({ appId, serverUrl: server.url, peer: "writer" });
      clients.push(writer);
      writer.connectTransport(server.url, { admin_secret: server.adminSecret });

      const inserted = writer.insert("todos", {
        title: { type: "Text", value: "websocket restart" },
        done: { type: "Boolean", value: true },
      });
      await waitForPromise(
        inserted.wait({ tier: "edge" }),
        "writer insert did not settle at edge before restart",
      );

      await writer.shutdown();
      clients.splice(clients.indexOf(writer), 1);
      await server.stop();
      server = null;

      server = await startLocalJazzServer({
        appId,
        port,
        dataDir,
        adminSecret,
      });

      const reader = await createClient({ appId, serverUrl: server.url, peer: "reader" });
      clients.push(reader);
      reader.connectTransport(server.url, { admin_secret: server.adminSecret });

      const replayedToSubscription = new Promise<string>((resolve) => {
        reader.subscribe(
          JSON.stringify({ table: "todos" }),
          (delta) => {
            for (const change of normalizeTestDelta(delta, schema)) {
              if ("row" in change && change.row?.id === inserted.value.id) {
                const firstValue = change.row.values[0];
                if (firstValue?.type === "Text") {
                  resolve(firstValue.value);
                }
              }
            }
          },
          { tier: "local" },
        );
      });
      await waitForPromise(
        replayedToSubscription,
        "reader subscription did not replay the persisted native runtime insert after restart",
      );

      const persistedRow = await waitFor(async () => {
        const rows = await reader.query(JSON.stringify({ table: "todos" }), { tier: "local" });
        return rows.find((row) => row.id === inserted.value.id);
      });

      expect(persistedRow).toMatchObject({
        id: inserted.value.id,
        values: [
          { type: "Text", value: "websocket restart" },
          { type: "Boolean", value: true },
        ],
      });
    },
    20_000,
  );

  maybeNapiIt(
    "hydrates routed large-value chunks before NAPI query and subscription encoding",
    async () => {
      globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;
      const appId = "00000000-0000-0000-0000-00000000c005";
      server = await startLocalJazzServer({
        appId,
        inMemory: true,
        adminSecret: "native-runtime-large-value-admin",
        schema: encodeSchema(largeValueSchema),
      });
      const writer = await createNapiClient({ appId, serverUrl: server.url, peer: "large-writer" });
      const reader = await createNapiClient({ appId, serverUrl: server.url, peer: "large-reader" });
      clients.push(writer, reader);
      writer.connectTransport(server.url, { admin_secret: server.adminSecret });
      reader.connectTransport(server.url, { admin_secret: server.adminSecret });

      const text = "routed text ".repeat(9_000);
      const bytes = Uint8Array.from({ length: 90_000 }, (_, index) => index % 251);

      const textWrite = await writer.insertStreaming(
        "values",
        { kind: { type: "Text", value: "text" } },
        "text",
        streamOf(text),
      );
      await waitForPromise(textWrite.wait({ tier: "edge" }), "streamed Text did not settle");
      const bytesWrite = await writer.insertStreaming(
        "values",
        { kind: { type: "Text", value: "bytes" } },
        "bytes",
        streamOf(bytes),
      );
      await waitForPromise(bytesWrite.wait({ tier: "edge" }), "streamed Bytea did not settle");
      const received = await waitFor(async () => {
        const rows = await reader.query(JSON.stringify({ table: "values" }), { tier: "edge" });
        return rows.length === 2 ? rows : undefined;
      }, 15_000);
      const values = new Map(
        received.map((row) => {
          const kind = row.values[0];
          if (kind?.type !== "Text") throw new Error("expected a Text kind discriminator");
          return [kind.value, row.values] as const;
        }),
      );
      expect(values.get("text")?.[1]).toEqual({ type: "Text", value: text });
      expect(values.get("bytes")?.[2]).toEqual({ type: "Bytea", value: bytes });

      // The NAPI subscription is independently encoded from one-shot rows.
      const subscription = new Promise<void>((resolve) => {
        reader.subscribe(
          JSON.stringify({ table: "values" }),
          (delta) => {
            const rows = normalizeTestDelta(delta, largeValueSchema);
            if (
              rows.some((change) => {
                if (!("row" in change) || !change.row) return false;
                const kind = change.row.values[0];
                return kind?.type === "Text" && kind.value === "bytes";
              })
            )
              resolve();
          },
          { tier: "edge" },
        );
      });
      await waitForPromise(subscription, "NAPI subscription did not publish hydrated large values");
    },
    30_000,
  );

  maybeIt("replays accepted BYTEA rows to a fresh websocket subscriber", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const appId = "00000000-0000-0000-0000-00000000c003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "native-runtime-bytea-convergence-admin",
      schema: encodeSchema(arraySchema),
    });

    const writer = await createClient({
      appId,
      serverUrl: server.url,
      peer: "bytea-writer",
      schema: arraySchema,
    });
    clients.push(writer);
    writer.connectTransport(server.url, { admin_secret: server.adminSecret });

    const inserted = writer.insert("arrays", {
      data: { type: "Bytea", value: Uint8Array.from([1, 2, 3, 4]) },
    });
    await waitForPromise(inserted.wait({ tier: "edge" }), "BYTEA insert did not settle at edge");
    await writer.shutdown();
    clients.splice(clients.indexOf(writer), 1);

    const reader = await createClient({
      appId,
      serverUrl: server.url,
      peer: "bytea-reader",
      schema: arraySchema,
    });
    clients.push(reader);
    reader.connectTransport(server.url, { admin_secret: server.adminSecret });

    const replayedToSubscription = new Promise<Uint8Array>((resolve) => {
      reader.subscribe(
        JSON.stringify({ table: "arrays" }),
        (delta) => {
          for (const change of normalizeTestDelta(delta, arraySchema)) {
            if ("row" in change && change.row?.id === inserted.value.id) {
              const firstValue = change.row.values[0];
              if (firstValue?.type === "Bytea") {
                resolve(firstValue.value);
              }
            }
          }
        },
        { tier: "local" },
      );
    });

    const bytes = await waitForPromise(
      replayedToSubscription,
      "fresh reader subscription did not replay accepted BYTEA row",
    );
    expect(Array.from(bytes)).toEqual([1, 2, 3, 4]);
  });

  maybeIt(
    "replays a restored row after insert-delete-restore to a fresh websocket subscriber",
    async () => {
      globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

      const appId = "00000000-0000-0000-0000-00000000c004";
      server = await startLocalJazzServer({
        appId,
        inMemory: true,
        adminSecret: "native-runtime-restore-convergence-admin",
        schema: encodeSchema(writableTodoSchema),
      });

      const writer = await createClient({
        appId,
        serverUrl: server.url,
        peer: "restore-writer",
      });
      clients.push(writer);
      writer.connectTransport(server.url, { admin_secret: server.adminSecret });

      const inserted = writer.insert("todos", {
        title: { type: "Text", value: "websocket before delete" },
        done: { type: "Boolean", value: false },
      });
      await waitForPromise(
        inserted.wait({ tier: "edge" }),
        "writer insert did not settle at edge before delete",
      );

      const deleted = writer.delete("todos", inserted.value.id);
      await waitForPromise(
        deleted.wait({ tier: "edge" }),
        "writer delete did not settle at edge before restore",
      );

      const restored = writer.restore("todos", inserted.value.id, {
        title: { type: "Text", value: "websocket restored row" },
        done: { type: "Boolean", value: true },
      });
      await waitForPromise(
        restored.wait({ tier: "edge" }),
        "writer restore did not settle at edge",
      );

      await writer.shutdown();
      clients.splice(clients.indexOf(writer), 1);

      const reader = await createClient({
        appId,
        serverUrl: server.url,
        peer: "restore-reader",
      });
      clients.push(reader);
      reader.connectTransport(server.url, { admin_secret: server.adminSecret });

      const replayedValues: string[] = [];
      const replayedToSubscription = new Promise<string>((resolve) => {
        reader.subscribe(
          JSON.stringify({ table: "todos" }),
          (delta) => {
            for (const change of normalizeTestDelta(delta, schema)) {
              if ("row" in change && change.row?.id === inserted.value.id) {
                const firstValue = change.row.values[0];
                if (firstValue?.type === "Text") {
                  replayedValues.push(firstValue.value);
                  if (firstValue.value === "websocket restored row") {
                    resolve(firstValue.value);
                  }
                }
              }
            }
          },
          { tier: "local" },
        );
      });

      await expect(
        waitForPromise(
          replayedToSubscription,
          `fresh reader subscription did not replay restored row; saw ${JSON.stringify(replayedValues)}`,
        ),
      ).resolves.toBe("websocket restored row");

      const restoredRow = await waitFor(async () => {
        const rows = await reader.query(JSON.stringify({ table: "todos" }), { tier: "local" });
        return rows.find((row) => row.id === inserted.value.id);
      });

      expect(restoredRow).toMatchObject({
        id: inserted.value.id,
        values: [
          { type: "Text", value: "websocket restored row" },
          { type: "Boolean", value: true },
        ],
      });
    },
    15_000,
  );
});

async function publishSchema(
  server: LocalJazzServerHandle,
): Promise<{ objectId: string; hash: string }> {
  return publishStoredSchema(server.url, {
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema,
  });
}

async function createClient({
  appId,
  serverUrl,
  peer,
  schema: clientSchema = schema,
}: {
  appId: string;
  serverUrl: string;
  peer: string;
  schema?: WasmSchema;
}): Promise<JazzClient> {
  const runtime = await createWasmRuntime(clientSchema, { appId, peerId: peer });
  return JazzClient.connectWithRuntime(runtime, {
    appId,
    schema: clientSchema,
    serverUrl,
  });
}

async function createNapiClient({
  appId,
  serverUrl,
  peer,
}: {
  appId: string;
  serverUrl: string;
  peer: string;
}): Promise<JazzClient> {
  const runtime = await createNapiNativeRuntimeAdapter(largeValueSchema, { appId, peerId: peer });
  return JazzClient.connectWithRuntime(runtime, { appId, schema: largeValueSchema, serverUrl });
}

function streamOf(...chunks: Array<string | Uint8Array>): ReadableStream<string | Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}

async function waitFor<T>(read: () => Promise<T | undefined>, timeoutMs = 5_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastValue: T | undefined;

  do {
    lastValue = await read();
    if (lastValue !== undefined) {
      return lastValue;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  } while (Date.now() < deadline);

  throw new Error(`Timed out waiting for native runtime convergence; last value: ${lastValue}`);
}

async function waitForPromise<T>(
  promise: Promise<T>,
  message: string,
  timeoutMs = 5_000,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

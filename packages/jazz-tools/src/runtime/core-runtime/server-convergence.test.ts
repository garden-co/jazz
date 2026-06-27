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
import { encodeSchema } from "./native-runtime-adapter.js";

const maybeIt = hasJazzWasmBuild() ? it : it.skip;
const previousWebSocket = globalThis.WebSocket;

const schema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
} satisfies WasmSchema;

const binaryLargeValueSchema = {
  binary_large_values: {
    columns: [{ name: "data", column_type: { type: "Bytea" }, nullable: false }],
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

  maybeIt(
    "syncs writes between two JazzClient connections through /apps/:app/ws",
    async () => {
      globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

      const appId = "00000000-0000-0000-0000-00000000c001";
      server = await startLocalJazzServer({
        appId,
        inMemory: true,
        adminSecret: "core-runtime-convergence-admin",
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
            if (!Array.isArray(delta)) return;
            for (const change of delta) {
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

      await waitForPromise(
        inserted.wait({ tier: "edge" }),
        "client A insert did not settle at edge",
      );
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
    },
    15_000,
  );

  maybeIt(
    "persists websocket writes across server restart",
    async () => {
      globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

      const tempRoot = await mkdtemp(join(tmpdir(), "jazz-core-runtime-restart-"));
      tempRoots.push(tempRoot);
      const dataDir = join(tempRoot, "server-data");
      const appId = "00000000-0000-0000-0000-00000000c002";
      const adminSecret = "core-runtime-restart-admin";

      server = await startLocalJazzServer({
        appId,
        dataDir,
        adminSecret,
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
        title: { type: "Text", value: "websocket dynamic activation" },
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
        body: JSON.stringify({ schema }),
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
            if (!Array.isArray(delta)) return;
            for (const change of delta) {
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

  maybeIt("replays accepted BYTEA rows to a fresh websocket subscriber", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const appId = "00000000-0000-0000-0000-00000000c003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-runtime-bytea-convergence-admin",
      schema: encodeSchema(binaryLargeValueSchema),
    });

    const writer = await createClient({
      appId,
      serverUrl: server.url,
      peer: "bytea-writer",
      schema: binaryLargeValueSchema,
    });
    clients.push(writer);
    writer.connectTransport(server.url, { admin_secret: server.adminSecret });

    const inserted = writer.insert("binary_large_values", {
      data: { type: "Bytea", value: Uint8Array.from([1, 2, 3, 4]) },
    });
    await waitForPromise(inserted.wait({ tier: "edge" }), "BYTEA insert did not settle at edge");
    await writer.shutdown();
    clients.splice(clients.indexOf(writer), 1);

    const reader = await createClient({
      appId,
      serverUrl: server.url,
      peer: "bytea-reader",
      schema: binaryLargeValueSchema,
    });
    clients.push(reader);
    reader.connectTransport(server.url, { admin_secret: server.adminSecret });

    const replayedToSubscription = new Promise<Uint8Array>((resolve) => {
      reader.subscribe(
        JSON.stringify({ table: "binary_large_values" }),
        (delta) => {
          if (!Array.isArray(delta)) return;
          for (const change of delta) {
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
        adminSecret: "core-runtime-restore-convergence-admin",
        schema: encodeSchema(schema),
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
            if (!Array.isArray(delta)) return;
            for (const change of delta) {
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
  const runtime = await createWasmRuntime(clientSchema, { appId, userBranch: peer });
  return JazzClient.connectWithRuntime(runtime, {
    appId,
    schema: clientSchema,
    serverUrl,
    userBranch: peer,
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

import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocket } from "undici";
import type { WasmSchema } from "../../drivers/types.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../../testing/index.js";
import { JazzClient } from "../client.js";
import { createWasmRuntime, hasJazzWasmBuild } from "../testing/wasm-runtime-test-utils.js";
import { encodeDirectSchema } from "./runtime.js";

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

describe("DirectWasmRuntime server convergence", () => {
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
        adminSecret: "direct-wasm-convergence-admin",
        schema: encodeDirectSchema(schema),
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
        title: { type: "Text", value: "direct websocket convergence" },
        done: { type: "Boolean", value: false },
      });

      await waitForPromise(
        inserted.wait({ tier: "edge" }),
        "client A insert did not settle at edge",
      );
      await waitForPromise(
        observedBySubscription,
        "client B subscription did not observe the direct WASM insert",
      );

      const convergedRows = await waitFor(async () => {
        const rows = await clientB.query(JSON.stringify({ table: "todos" }), { tier: "local" });
        return rows.find((row) => row.id === inserted.value.id);
      });

      expect(convergedRows).toMatchObject({
        id: inserted.value.id,
        values: [
          { type: "Text", value: "direct websocket convergence" },
          { type: "Boolean", value: false },
        ],
      });
    },
    15_000,
  );

  maybeIt(
    "persists direct websocket writes across server restart",
    async () => {
      globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

      const tempRoot = await mkdtemp(join(tmpdir(), "jazz-direct-wasm-restart-"));
      tempRoots.push(tempRoot);
      const dataDir = join(tempRoot, "server-data");
      const appId = "00000000-0000-0000-0000-00000000c002";
      const adminSecret = "direct-wasm-restart-admin";

      server = await startLocalJazzServer({
        appId,
        dataDir,
        adminSecret,
        schema: encodeDirectSchema(schema),
      });

      const writer = await createClient({ appId, serverUrl: server.url, peer: "writer" });
      clients.push(writer);
      writer.connectTransport(server.url, { admin_secret: server.adminSecret });

      const inserted = writer.insert("todos", {
        title: { type: "Text", value: "direct websocket restart" },
        done: { type: "Boolean", value: true },
      });
      await waitForPromise(
        inserted.wait({ tier: "edge" }),
        "writer insert did not settle at edge before restart",
      );

      await writer.shutdown();
      clients.splice(clients.indexOf(writer), 1);
      const port = server.port;
      await server.stop();
      server = null;

      server = await startLocalJazzServer({
        appId,
        port,
        dataDir,
        adminSecret,
        schema: encodeDirectSchema(schema),
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
        "reader subscription did not replay the persisted direct WASM insert after restart",
      );

      const persistedRow = await waitFor(async () => {
        const rows = await reader.query(JSON.stringify({ table: "todos" }), { tier: "local" });
        return rows.find((row) => row.id === inserted.value.id);
      });

      expect(persistedRow).toMatchObject({
        id: inserted.value.id,
        values: [
          { type: "Text", value: "direct websocket restart" },
          { type: "Boolean", value: true },
        ],
      });
    },
    20_000,
  );
});

async function createClient({
  appId,
  serverUrl,
  peer,
}: {
  appId: string;
  serverUrl: string;
  peer: string;
}): Promise<JazzClient> {
  const runtime = await createWasmRuntime(schema, { appId, userBranch: peer });
  return JazzClient.connectWithRuntime(runtime, {
    appId,
    schema,
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

  throw new Error(`Timed out waiting for direct WASM convergence; last value: ${lastValue}`);
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

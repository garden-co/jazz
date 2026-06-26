import { afterEach, describe, expect, it } from "vitest";
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

  afterEach(async () => {
    await Promise.allSettled(clients.splice(0).map((client) => client.shutdown()));
    await server?.stop();
    server = null;
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

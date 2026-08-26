import { access } from "node:fs/promises";
import { WebSocket } from "undici";
import { afterEach, describe, expect, it } from "vitest";
import { createJazzContext, type JazzContext } from "../backend/index.js";
import { schema as s } from "../index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "./dev-server.js";
import { getAvailablePort } from "./test-helpers.js";

const relayReceiptApp = s.defineApp({
  receipts: s.table({
    message: s.string(),
  }),
});

const relayReceiptPermissions = s.definePermissions(relayReceiptApp, ({ policy }) => [
  policy.receipts.allowRead.always(),
  policy.receipts.allowInsert.always(),
  policy.receipts.allowUpdate.never(),
  policy.receipts.allowDelete.never(),
]);

const relayReceiptServerSchema = encodeSchema(
  mergePermissionsIntoWasmSchema(relayReceiptApp.wasmSchema, relayReceiptPermissions),
);

describe("dev-server re-export compatibility", () => {
  it("exports startLocalJazzServer and deploy from jazz-tools/testing path", async () => {
    const testing = await import("../testing/index.js");
    expect(typeof testing.startLocalJazzServer).toBe("function");
    expect(typeof testing.deploy).toBe("function");
  });

  it("exports the same functions from dev/index.ts", async () => {
    const dev = await import("./index.js");
    expect(typeof dev.startLocalJazzServer).toBe("function");
    expect(typeof dev.watchSchema).toBe("function");
    expect(typeof dev.pushSchema).toBe("function");
    expect(typeof dev.pushPermissions).toBe("function");
    expect(typeof dev.pushMigration).toBe("function");
    expect(typeof dev.deploy).toBe("function");
  });

  it("testing and dev export the same startLocalJazzServer reference", async () => {
    const testing = await import("../testing/index.js");
    const dev = await import("./index.js");
    expect(testing.startLocalJazzServer).toBe(dev.startLocalJazzServer);
    expect(testing.deploy).toBe(dev.deploy);
  });
});

describe("startLocalJazzServer via JazzServer", () => {
  let handle: LocalJazzServerHandle | null = null;

  afterEach(async () => {
    if (handle) {
      await handle.stop();
      handle = null;
    }
  });

  it("starts a server and returns a working handle", async () => {
    const port = await getAvailablePort();
    handle = await startLocalJazzServer({ port, adminSecret: "test-admin" });

    expect(handle.port).toBe(port);
    expect(handle.url).toBe(`http://127.0.0.1:${port}`);
    expect(handle.adminSecret).toBe("test-admin");
    expect(handle.backendSecret).toEqual(expect.any(String));

    const healthResponse = await fetch(`${handle.url}/health`);
    expect(healthResponse.ok).toBe(true);
  }, 30_000);

  it("stops the server cleanly", async () => {
    const port = await getAvailablePort();
    handle = await startLocalJazzServer({ port });
    const url = handle.url;
    await handle.stop();
    handle = null;

    await expect(fetch(`${url}/health`).then((r) => r.ok)).rejects.toThrow();
  }, 30_000);

  it("passes edge upstream options through JazzServer with admin secret only", async () => {
    const port = await getAvailablePort();
    handle = await startLocalJazzServer({
      port,
      upstreamUrl: "ws://127.0.0.1:9",
      adminSecret: "admin-secret",
      inMemory: true,
    });

    expect(handle.port).toBe(port);
    const healthResponse = await fetch(`${handle.url}/health`);
    expect(healthResponse.ok).toBe(true);
  }, 30_000);

  it("makes a backend write through Edge globally visible from Core", async () => {
    const previousWebSocket = globalThis.WebSocket;
    globalThis.WebSocket = WebSocket as unknown as typeof globalThis.WebSocket;

    const appId = "00000000-0000-0000-0000-00000000d004";
    const adminSecret = "dev-server-relay-receipt-admin";
    let core: LocalJazzServerHandle | null = null;
    let edge: LocalJazzServerHandle | null = null;
    let edgeContext: JazzContext | null = null;
    let coreContext: JazzContext | null = null;

    try {
      const corePort = await getAvailablePort();
      edge = await startLocalJazzServer({
        appId,
        port: await getAvailablePort(),
        inMemory: true,
        adminSecret,
        backendSecret: "dev-server-relay-receipt-edge-backend",
        upstreamUrl: `http://127.0.0.1:${corePort}`,
        schema: relayReceiptServerSchema,
      });

      edgeContext = createJazzContext({
        appId,
        app: relayReceiptApp,
        permissions: relayReceiptPermissions,
        driver: { type: "memory" },
        serverUrl: edge.url,
        backendSecret: edge.backendSecret,
        env: "dev-server-relay-receipt-edge",
        tier: "edge",
      });
      await withTimeout(
        edgeContext.asBackend().all(relayReceiptApp.receipts, { tier: "edge" }),
        15_000,
        "Fixed-schema Edge did not become ready for its public NAPI client",
      );

      core = await startLocalJazzServer({
        appId,
        port: corePort,
        inMemory: true,
        adminSecret,
        backendSecret: "dev-server-relay-receipt-core-backend",
      });
      await deploy({
        serverUrl: core.url,
        appId,
        adminSecret,
        schema: relayReceiptApp,
        permissions: relayReceiptPermissions,
      });

      coreContext = createJazzContext({
        appId,
        app: relayReceiptApp,
        permissions: relayReceiptPermissions,
        driver: { type: "memory" },
        serverUrl: core.url,
        backendSecret: core.backendSecret,
        env: "dev-server-relay-receipt-core",
        tier: "global",
      });

      const coreReader = coreContext.asBackend();
      const initialCoreRows = await withTimeout(
        coreReader.all(relayReceiptApp.receipts, {
          tier: "global",
          propagation: "full",
        }),
        15_000,
        "Core initial settled read did not complete",
      );
      expect(initialCoreRows).toEqual([]);

      const edgeWriter = edgeContext.withAttribution(
        "https://dev-server-relay-receipt.example",
        "writer",
      );
      const write = edgeWriter.insert(relayReceiptApp.receipts, {
        message: "globally relayed",
      });
      const globallySettled = await withTimeout(
        write.wait({ tier: "global" }),
        15_000,
        "Edge write did not settle globally",
      );

      const coreRows = await withTimeout(
        coreReader.all(relayReceiptApp.receipts, {
          tier: "global",
          propagation: "full",
        }),
        15_000,
        "Core settled read did not complete",
      );

      expect(globallySettled).toMatchObject({
        id: write.value.id,
        message: "globally relayed",
      });
      expect(coreRows).toContainEqual(
        expect.objectContaining({
          id: write.value.id,
          message: "globally relayed",
        }),
      );
    } finally {
      await Promise.allSettled([edgeContext?.shutdown(), coreContext?.shutdown()]);
      if (edge) {
        await edge.stop().catch(() => undefined);
      }
      if (core) {
        await core.stop().catch(() => undefined);
      }
      globalThis.WebSocket = previousWebSocket;
    }
  }, 60_000);

  it("uses an isolated temp data dir by default and cleans it up on stop", async () => {
    let first: LocalJazzServerHandle | null = null;
    let second: LocalJazzServerHandle | null = null;

    try {
      first = await startLocalJazzServer();
      const firstDataDir = first.dataDir;
      expect(first.adminSecret).toEqual(expect.any(String));
      expect(first.backendSecret).toEqual(expect.any(String));
      expect(firstDataDir).not.toBe("./data");
      await access(firstDataDir);

      await first.stop();
      first = null;
      await expect(access(firstDataDir)).rejects.toThrow();

      second = await startLocalJazzServer();
      expect(second.dataDir).not.toBe(firstDataDir);
      await access(second.dataDir);
    } finally {
      if (first) {
        await first.stop();
      }
      if (second) {
        await second.stop();
      }
    }
  }, 30_000);
});

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
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

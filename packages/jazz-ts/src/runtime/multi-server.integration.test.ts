import { spawn, type ChildProcess } from "node:child_process";
import { createHmac, randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { translateQuery } from "./query-adapter.js";
import { sendSyncPayload } from "./sync-transport.js";
import { hasGrooveWasmBuild } from "./testing/wasm-runtime-test-utils.js";
import type { WasmSchema } from "../drivers/types.js";

type AppContext = import("./context.js").AppContext;
type JazzClient = import("./client.js").JazzClient;
type Row = import("./client.js").Row;

const INTERNAL_API_SECRET = "jazz-ts-internal-api-secret";
const SECRET_HASH_KEY = "jazz-ts-secret-hash-key";
const ADMIN_SECRET = "jazz-ts-admin-secret";
const BACKEND_SECRET = "jazz-ts-backend-secret";
const JWT_KID = "jazz-ts-kid";
const JWT_SECRET = "jazz-ts-jwt-secret";
const TEST_SCHEMA: WasmSchema = {
  tables: {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
  },
};

type MultiServerConfig = {
  dataRoot: string;
};

type MultiServerHandle = {
  child: ChildProcess;
  port: number;
  baseUrl: string;
};

type CreatedApp = {
  app_id: string;
};

const tempDirsToCleanup: string[] = [];

function allocTempDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tempDirsToCleanup.push(dir);
  return dir;
}

function base64url(input: Buffer | string): string {
  const encoded = (input instanceof Buffer ? input : Buffer.from(input)).toString("base64");
  return encoded.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function signJwt(sub: string, secret: string): string {
  const header = {
    alg: "HS256",
    typ: "JWT",
    kid: JWT_KID,
  };
  const payload = {
    sub,
    claims: {},
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const signedPart = `${headerB64}.${payloadB64}`;
  const sig = createHmac("sha256", secret).update(signedPart).digest();
  return `${signedPart}.${base64url(sig)}`;
}

function makeSyncPayload() {
  return {
    ObjectUpdated: {
      object_id: randomUUID(),
      metadata: null,
      branch_name: "main",
      commits: [],
    },
  };
}

function findMultiServerBinary(): string | null {
  const runtimeDir = dirname(fileURLToPath(import.meta.url));
  const basePath = join(runtimeDir, "../../../../target/debug/jazz-multi-server");
  if (existsSync(basePath)) return basePath;
  if (existsSync(`${basePath}.exe`)) return `${basePath}.exe`;
  return null;
}

function assertIntegrationPrerequisites(): void {
  const hasWasm = hasGrooveWasmBuild();
  const binaryPath = findMultiServerBinary();
  if (hasWasm && binaryPath) return;

  const missing: string[] = [];
  if (!hasWasm) {
    missing.push("missing Groove WASM runtime artifacts");
  }
  if (!binaryPath) {
    missing.push("missing jazz-multi-server debug binary at target/debug/jazz-multi-server");
  }

  throw new Error(
    [
      "Multi-server TS integration prerequisites are missing:",
      ...missing.map((entry) => `- ${entry}`),
      "Build prerequisites, then rerun tests:",
      "1. pnpm --filter @jazz/rust build:crates",
      "2. cargo build -p jazz-multi-server",
    ].join("\n"),
  );
}

function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("failed to allocate free port"));
        return;
      }
      const port = address.port;
      server.close((err) => {
        if (err) reject(err);
        else resolve(port);
      });
    });
    server.on("error", reject);
  });
}

async function waitForHealth(baseUrl: string): Promise<void> {
  const healthUrl = `${baseUrl}/health`;
  for (let i = 0; i < 100; i++) {
    try {
      const response = await fetch(healthUrl);
      if (response.ok) return;
    } catch {
      // Not ready yet.
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`multi-server failed health check at ${healthUrl}`);
}

async function startMultiServer(config: MultiServerConfig): Promise<MultiServerHandle> {
  const binary = findMultiServerBinary();
  if (!binary) {
    throw new Error("jazz-multi-server binary not found");
  }

  const port = await getFreePort();
  const child = spawn(
    binary,
    [
      "--port",
      String(port),
      "--data-root",
      config.dataRoot,
      "--internal-api-secret",
      INTERNAL_API_SECRET,
      "--secret-hash-key",
      SECRET_HASH_KEY,
      "--worker-threads",
      "1",
    ],
    {
      stdio: ["ignore", "pipe", "pipe"],
      env: process.env,
    },
  );

  const baseUrl = `http://127.0.0.1:${port}`;
  await waitForHealth(baseUrl);
  return { child, port, baseUrl };
}

async function stopProcess(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.killed) return;
  child.kill("SIGTERM");
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
      }
      resolve();
    }, 2000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function createApp(baseUrl: string, jwksEndpoint: string): Promise<CreatedApp> {
  const response = await fetch(`${baseUrl}/internal/apps`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Internal-Secret": INTERNAL_API_SECRET,
    },
    body: JSON.stringify({
      app_name: "jazz-ts-multi-server-test",
      jwks_endpoint: jwksEndpoint,
      backend_secret: BACKEND_SECRET,
      admin_secret: ADMIN_SECRET,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`create app failed ${response.status}: ${text}`);
  }

  return (await response.json()) as CreatedApp;
}

async function waitForRows(
  client: JazzClient,
  queryJson: string,
  predicate: (rows: Row[]) => boolean,
  timeoutMs = 20000,
  settledTier: "edge" | undefined = "edge",
): Promise<Row[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: Row[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await client.query(queryJson, settledTier);
      if (predicate(rows)) return rows;
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }
    await new Promise((r) => setTimeout(r, 150));
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `timed out waiting for predicate; lastRows=${JSON.stringify(lastRows)}, lastError=${lastErrorMessage}`,
  );
}

async function connectClient(context: AppContext): Promise<JazzClient> {
  const [clientMod, runtimeUtils] = await Promise.all([
    import("./client.js"),
    import("./testing/wasm-runtime-test-utils.js"),
  ]);

  const runtime = await runtimeUtils.createWasmRuntime(context.schema, {
    appId: context.appId,
    env: context.env,
    userBranch: context.userBranch,
  });

  return clientMod.JazzClient.connectWithRuntime(runtime, context);
}

class JwksServer {
  private server: Server;
  readonly url: string;

  private constructor(server: Server, url: string) {
    this.server = server;
    this.url = url;
  }

  static async start(secret: string): Promise<JwksServer> {
    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      if (req.url !== "/jwks") {
        res.statusCode = 404;
        res.end("not found");
        return;
      }

      const body = JSON.stringify({
        keys: [
          {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64url(secret),
          },
        ],
      });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(body);
    });

    const port = await getFreePort();
    await new Promise<void>((resolve, reject) => {
      server.listen(port, "127.0.0.1", (err?: unknown) => {
        if (err) reject(err);
        else resolve();
      });
    });

    return new JwksServer(server, `http://127.0.0.1:${port}/jwks`);
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

function makeContext(appId: string, serverUrl: string, jwtToken: string): AppContext {
  return {
    appId,
    schema: TEST_SCHEMA,
    serverUrl,
    serverPathPrefix: `/apps/${appId}`,
    env: "test",
    userBranch: "main",
    jwtToken,
    adminSecret: ADMIN_SECRET,
  };
}

afterEach(() => {
  while (tempDirsToCleanup.length > 0) {
    const dir = tempDirsToCleanup.pop()!;
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      // best effort cleanup
    }
  }
});

describe("multi-server integration (Jazz TS)", () => {
  beforeAll(() => {
    assertIntegrationPrerequisites();
  });

  it("routes sync requests through serverPathPrefix with JWT auth", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-multi-server-");
    const server = await startMultiServer({ dataRoot });

    try {
      const app = await createApp(server.baseUrl, jwks.url);
      const pathPrefix = `/apps/${app.app_id}`;

      await sendSyncPayload(
        server.baseUrl,
        makeSyncPayload(),
        { jwtToken: signJwt("valid-user", JWT_SECRET), pathPrefix },
        "[valid] ",
      );

      await expect(
        sendSyncPayload(
          server.baseUrl,
          makeSyncPayload(),
          { jwtToken: signJwt("invalid-user", "wrong-secret"), pathPrefix },
          "[invalid] ",
        ),
      ).rejects.toThrow("401");
    } finally {
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("resolves empty settled-tier query snapshots", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-multi-server-empty-query-");
    const server = await startMultiServer({ dataRoot });
    const queryAllTodos = translateQuery(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      }),
      TEST_SCHEMA,
    );

    let client: JazzClient | null = null;
    try {
      const app = await createApp(server.baseUrl, jwks.url);
      client = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("empty-snapshot", JWT_SECRET)),
      );

      const rows = await waitForRows(
        client,
        queryAllTodos,
        (all) => all.length === 0,
        20000,
        "edge",
      );
      expect(rows).toEqual([]);
    } finally {
      if (client) await client.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("syncs queries and mutations between two TS clients via multi-server", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-multi-server-");
    const server = await startMultiServer({ dataRoot });

    const queryAllTodos = translateQuery(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      }),
      TEST_SCHEMA,
    );

    let clientA: JazzClient | null = null;
    let clientB: JazzClient | null = null;

    try {
      const app = await createApp(server.baseUrl, jwks.url);
      clientA = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("a", JWT_SECRET)),
      );
      clientB = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("b", JWT_SECRET)),
      );

      const rowId = await clientA.createPersisted(
        "todos",
        [
          { type: "Text", value: "shared-item" },
          { type: "Boolean", value: false },
        ],
        "edge",
      );

      const rowsAfterCreate = await waitForRows(clientB, queryAllTodos, (rows) =>
        rows.some((row) => row.id === rowId),
      );
      const createdRow = rowsAfterCreate.find((row) => row.id === rowId);
      expect(createdRow?.values[0]).toEqual({ type: "Text", value: "shared-item" });

      await clientA.updatePersisted(rowId, { done: { type: "Boolean", value: true } }, "edge");
      const rowsAfterUpdate = await waitForRows(clientB, queryAllTodos, (rows) => {
        const row = rows.find((r) => r.id === rowId);
        return Boolean(row && row.values[1]?.type === "Boolean" && row.values[1].value === true);
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.values[1]).toEqual({ type: "Boolean", value: true });

      await clientA.deletePersisted(rowId, "edge");
      await waitForRows(clientB, queryAllTodos, (rows) => !rows.some((row) => row.id === rowId));
    } finally {
      if (clientA) await clientA.shutdown();
      if (clientB) await clientB.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("resyncs data from multi-server after server restart", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-multi-server-restart-");
    const queryAllTodos = translateQuery(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      }),
      TEST_SCHEMA,
    );

    const appId = await (async () => {
      const server = await startMultiServer({ dataRoot });
      let writer: JazzClient | null = null;
      try {
        const app = await createApp(server.baseUrl, jwks.url);
        writer = await connectClient(
          makeContext(app.app_id, server.baseUrl, signJwt("writer", JWT_SECRET)),
        );
        await writer.createPersisted(
          "todos",
          [
            { type: "Text", value: "persisted-item" },
            { type: "Boolean", value: false },
          ],
          "edge",
        );
        await waitForRows(writer, queryAllTodos, (rows) => rows.length >= 1, 15000);
        return app.app_id;
      } finally {
        if (writer) await writer.shutdown();
        await stopProcess(server.child);
      }
    })();

    const restarted = await startMultiServer({ dataRoot });
    let reader: JazzClient | null = null;
    try {
      reader = await connectClient(
        makeContext(appId, restarted.baseUrl, signJwt("reader", JWT_SECRET)),
      );
      const rows = await waitForRows(reader, queryAllTodos, (all) => all.length >= 1, 20000);
      expect(
        rows.some(
          (row) => row.values[0]?.type === "Text" && row.values[0].value === "persisted-item",
        ),
      ).toBe(true);
    } finally {
      if (reader) await reader.shutdown();
      await stopProcess(restarted.child);
      await jwks.stop();
    }
  }, 90000);
});

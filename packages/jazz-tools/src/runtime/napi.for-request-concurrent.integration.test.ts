import { createHmac, randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer as createHttpServer, type Server as HttpServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import { pushSchemaCatalogue, startLocalJazzServer } from "../testing/local-jazz-server.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type PolicyTodo = {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  owner_id: string;
};

type PolicyTodoInit = {
  title: string;
  done: boolean;
  description?: string;
  owner_id: string;
};

type TempRuntimeData = {
  dataRoot: string;
  dataPath: string;
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const JWT_KID = "napi-test-kid";
const JWT_SECRET = "napi-test-secret";

const TODO_SERVER_SCHEMA_DIR = fileURLToPath(
  new URL("../../../../examples/todo-server-ts", import.meta.url),
);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function base64Url(input: Buffer | string): string {
  const encoded = (input instanceof Buffer ? input : Buffer.from(input)).toString("base64");
  return encoded.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
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
          keys: [{ kty: "oct", kid: JWT_KID, k: base64Url(secret) }],
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

async function createTempRuntimeData(prefix: string): Promise<TempRuntimeData> {
  const dataRoot = await mkdtemp(join(tmpdir(), prefix));
  return { dataRoot, dataPath: join(dataRoot, "runtime.db") };
}

async function cleanupTempRuntimeData(data: TempRuntimeData | null): Promise<void> {
  if (!data) return;
  await rm(data.dataRoot, { recursive: true, force: true });
}

async function settleAsyncSyncWork(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
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
    if (timeoutId) clearTimeout(timeoutId);
  }
}

let todoServerProjectPromise: Promise<LoadedSchemaProject> | null = null;

async function loadTodoServerProject(): Promise<LoadedSchemaProject> {
  if (!todoServerProjectPromise) {
    todoServerProjectPromise = loadCompiledSchema(TODO_SERVER_SCHEMA_DIR);
  }
  return await todoServerProjectPromise;
}

function makePolicyTodosTable(schema: WasmSchema): TableProxy<PolicyTodo, PolicyTodoInit> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _initType: undefined as unknown as PolicyTodoInit,
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

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await loadNapiModule();
});

describe("forRequest concurrent session isolation", () => {
  it("isolates concurrent forRequest sessions on the same context — alice and bob see only their own rows", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-concurrent-backend-secret";
    const adminSecret = "napi-concurrent-admin-secret";
    const scopeTag = `concurrent-scope-${randomUUID()}`;
    let runtimeData: TempRuntimeData | null = null;

    const jwks = await JwksServer.start(JWT_SECRET);
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });

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
      const scopedQuery = makePolicyTodosByDescriptionQuery(todoServerSchema, scopeTag);

      runtimeData = await createTempRuntimeData("jazz-napi-concurrent-request-");
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

      // Obtain session-scoped Db handles for alice and bob concurrently from
      // the same shared context — this is the pattern a real server would use.
      const [aliceDb, bobDb] = await Promise.all([
        context.forRequest({
          headers: { authorization: `Bearer ${makeJwt({ sub: "alice" })}` },
        }),
        context.forRequest({
          headers: { authorization: `Bearer ${makeJwt({ sub: "bob" })}` },
        }),
      ]);

      // Fire writes for both users in parallel.
      await Promise.all([
        withTimeout(
          aliceDb.insertDurable(
            policyTodosTable,
            { title: "alice-todo", done: false, description: scopeTag, owner_id: "alice" },
            { tier: "edge" },
          ),
          10_000,
          "alice insert timed out",
        ),
        withTimeout(
          bobDb.insertDurable(
            policyTodosTable,
            { title: "bob-todo", done: false, description: scopeTag, owner_id: "bob" },
            { tier: "edge" },
          ),
          10_000,
          "bob insert timed out",
        ),
      ]);

      // Alice's scoped Db should only surface her own row.
      await vi.waitFor(
        async () => {
          const rows = await withTimeout(
            aliceDb.all(scopedQuery, { tier: "edge" }),
            10_000,
            "alice read timed out",
          );
          expect(rows.map((r) => r.title).sort()).toEqual(["alice-todo"]);
        },
        { timeout: 20_000 },
      );

      // Bob's scoped Db should only surface his own row.
      await vi.waitFor(
        async () => {
          const rows = await withTimeout(
            bobDb.all(scopedQuery, { tier: "edge" }),
            10_000,
            "bob read timed out",
          );
          expect(rows.map((r) => r.title).sort()).toEqual(["bob-todo"]);
        },
        { timeout: 20_000 },
      );

      // Cross-user write rejection: alice and bob must not be able to insert
      // rows owned by each other, even when their requests are in flight concurrently.
      await Promise.all([
        expect(
          aliceDb.insertDurable(
            policyTodosTable,
            { title: "alice-as-bob", done: false, description: scopeTag, owner_id: "bob" },
            { tier: "edge" },
          ),
        ).rejects.toThrow(),
        expect(
          bobDb.insertDurable(
            policyTodosTable,
            { title: "bob-as-alice", done: false, description: scopeTag, owner_id: "alice" },
            { tier: "edge" },
          ),
        ).rejects.toThrow(),
      ]);

      // A fresh forRequest call for alice (simulating a later HTTP request)
      // must still be isolated from bob's data.
      const aliceDb2 = await context.forRequest({
        headers: { authorization: `Bearer ${makeJwt({ sub: "alice" })}` },
      });
      await vi.waitFor(
        async () => {
          const rows = await withTimeout(
            aliceDb2.all(scopedQuery, { tier: "edge" }),
            10_000,
            "alice2 read timed out",
          );
          expect(rows.map((r) => r.title).sort()).toEqual(["alice-todo"]);
        },
        { timeout: 20_000 },
      );
    } finally {
      if (context) await context.shutdown();
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
      await jwks.stop();
    }
  }, 60_000);
});

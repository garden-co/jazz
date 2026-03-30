import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { CompiledPermissions } from "../permissions/index.js";
import type { AppContext, Session } from "../runtime/context.js";
import { createJazzContext } from "./create-jazz-context.js";

const mocks = vi.hoisted(() => {
  const resolveLocalAuthDefaults = vi.fn();
  const runtimeCtor = vi.fn();
  const inMemoryRuntimeCtor = vi.fn();
  const runtimeInstances: Array<{ flush: ReturnType<typeof vi.fn> }> = [];
  const createdDbs: Array<{ kind: string; client: unknown; session?: Session }> = [];
  const clients: Array<{
    asBackend: ReturnType<typeof vi.fn>;
    shutdown: ReturnType<typeof vi.fn>;
  }> = [];
  const connectWithRuntime = vi.fn((_runtime: unknown, _context: AppContext) => {
    const client = {
      asBackend: vi.fn(function (this: unknown) {
        return this;
      }),
      shutdown: vi.fn(async () => undefined),
    };
    clients.push(client);
    return client;
  });
  const createDbFromClient = vi.fn((_config: unknown, client: unknown, session?: Session) => {
    const db = {
      kind: session ? "scoped-db" : "db",
      client,
      ...(session ? { session } : {}),
    };
    createdDbs.push(db);
    return db;
  });

  class MockNapiRuntime {
    readonly flush = vi.fn();

    constructor(
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      dataPath: string,
      tier?: string,
    ) {
      runtimeCtor(schemaJson, appId, env, userBranch, dataPath, tier);
      runtimeInstances.push(this);
    }

    static inMemory(
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      tier?: string,
    ) {
      inMemoryRuntimeCtor(schemaJson, appId, env, userBranch, tier);
      const instance = new MockNapiRuntime(schemaJson, appId, env, userBranch, "__memory__", tier);
      return instance;
    }
  }

  class MockJazzClient {
    static connectWithRuntime = connectWithRuntime;
  }

  return {
    MockNapiRuntime,
    MockJazzClient,
    resolveLocalAuthDefaults,
    runtimeCtor,
    inMemoryRuntimeCtor,
    runtimeInstances,
    connectWithRuntime,
    clients,
    createDbFromClient,
    createdDbs,
    reset() {
      resolveLocalAuthDefaults.mockReset();
      runtimeCtor.mockReset();
      inMemoryRuntimeCtor.mockReset();
      runtimeInstances.length = 0;
      connectWithRuntime.mockClear();
      clients.length = 0;
      createDbFromClient.mockClear();
      createdDbs.length = 0;
    },
  };
});

vi.mock("jazz-napi", () => ({
  NapiRuntime: mocks.MockNapiRuntime,
}));

vi.mock("../runtime/client.js", async () => {
  const actual = await vi.importActual("../runtime/client.js");
  return {
    ...actual,
    JazzClient: mocks.MockJazzClient,
  };
});

vi.mock("../runtime/local-auth.js", () => ({
  resolveLocalAuthDefaults: mocks.resolveLocalAuthDefaults,
}));

vi.mock("../runtime/db.js", () => ({
  createDbFromClient: mocks.createDbFromClient,
}));

const SCHEMA_A: WasmSchema = {};
const SCHEMA_B: WasmSchema = { todos: { columns: [] } };
const TODO_PERMISSIONS: CompiledPermissions = {
  todos: {
    select: { using: { type: "True" } },
  },
};

function makeJwt(payload: Record<string, unknown>): string {
  const encode = (value: unknown) =>
    Buffer.from(JSON.stringify(value), "utf8")
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/g, "");

  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.signature`;
}

describe("backend/create-jazz-context", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.resolveLocalAuthDefaults.mockImplementation((config) => config);
  });

  it("BC-U01: lazily initializes runtime/client on first access", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(mocks.runtimeCtor).not.toHaveBeenCalled();
    expect(mocks.connectWithRuntime).not.toHaveBeenCalled();

    const dbA = context.db();
    const dbB = context.db();

    expect(dbA).not.toBe(dbB);
    expect(mocks.runtimeCtor).toHaveBeenCalledTimes(1);
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(1);
    expect(mocks.createDbFromClient).toHaveBeenCalledTimes(2);
    expect(mocks.createdDbs[0]?.client).toBe(mocks.createdDbs[1]?.client);
    expect(mocks.runtimeCtor).toHaveBeenCalledWith(
      serializeRuntimeSchema(SCHEMA_A),
      "server-app",
      "dev",
      "main",
      "/tmp/jazz.db",
      "edge",
    );
  });

  it("BC-U02: supports high-level db/backend/request/session helpers", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
      serverUrl: "http://localhost:1625",
      backendSecret: "secret",
    });

    const req = {
      header: (name: string) =>
        name === "authorization" ? `Bearer ${makeJwt({ sub: "u1" })}` : undefined,
    };
    const session: Session = { user_id: "u1", claims: {} };

    const db = context.db();
    const backendDb = context.asBackend();
    const requestDb = context.forRequest(req);
    const sessionDb = context.forSession(session);

    expect(db).toEqual({
      kind: "db",
      client: mocks.clients[0]!,
    });
    expect(backendDb).toEqual({
      kind: "db",
      client: mocks.clients[0]!,
    });
    expect(requestDb).toEqual({
      kind: "scoped-db",
      client: mocks.clients[0]!,
      session: { user_id: "u1", claims: {} },
    });
    expect(sessionDb).toEqual({
      kind: "scoped-db",
      client: mocks.clients[0]!,
      session,
    });
    expect(mocks.clients).toHaveLength(1);
    expect(mocks.clients[0]!.asBackend).toHaveBeenCalledTimes(3);
    expect(mocks.createDbFromClient).toHaveBeenCalledTimes(4);
  });

  it("BC-U03: request/session helpers work locally without backend sync config", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    const req = {
      header: (name: string) =>
        name === "authorization" ? `Bearer ${makeJwt({ sub: "u1" })}` : undefined,
    };
    const session: Session = { user_id: "u1", claims: {} };

    expect(() => context.forRequest(req)).not.toThrow();
    expect(() => context.forSession(session)).not.toThrow();
    expect(mocks.clients[0]!.asBackend).not.toHaveBeenCalled();
  });

  it("BC-U04: merges compiled permissions into the runtime schema", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: {
        wasmSchema: {
          todos: {
            columns: [],
          },
        },
      },
      permissions: TODO_PERMISSIONS,
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    context.db();

    expect(mocks.runtimeCtor).toHaveBeenCalledWith(
      serializeRuntimeSchema({
        todos: {
          columns: [],
          policies: TODO_PERMISSIONS.todos as any,
        },
      }),
      "server-app",
      "dev",
      "main",
      "/tmp/jazz.db",
      "edge",
    );
  });

  it("BC-U04: throws when no schema source is available", () => {
    const context = createJazzContext({
      appId: "server-app",
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(() => context.db()).toThrow("No schema source provided");
  });

  it("BC-U05: rejects switching to a different schema after initialization", () => {
    const context = createJazzContext({
      appId: "server-app",
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    context.db({ wasmSchema: SCHEMA_A });

    expect(() => context.db({ wasmSchema: SCHEMA_B })).toThrow(
      "already initialized with a different schema",
    );
  });

  it("BC-U06: flush is safe before init and delegates after init", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(() => context.flush()).not.toThrow();
    context.db();
    context.flush();

    expect(mocks.runtimeInstances).toHaveLength(1);
    expect(mocks.runtimeInstances[0]!.flush).toHaveBeenCalledTimes(1);
  });

  it("BC-U07: shutdown releases client and allows re-init", async () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    context.db();
    expect(mocks.clients).toHaveLength(1);

    await context.shutdown();
    expect(mocks.clients[0]!.shutdown).toHaveBeenCalledTimes(1);

    context.db();
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(2);
    expect(mocks.runtimeCtor).toHaveBeenCalledTimes(2);
  });

  it("BC-U08: uses in-memory runtime when driver.type is memory", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "memory" },
      serverUrl: "http://localhost:1625",
    });

    context.db();

    expect(mocks.inMemoryRuntimeCtor).toHaveBeenCalledTimes(1);
    expect(mocks.runtimeCtor).toHaveBeenCalledWith(
      serializeRuntimeSchema(SCHEMA_A),
      "server-app",
      "dev",
      "main",
      "__memory__",
      "edge",
    );
  });

  it("BC-U09: rejects memory driver without serverUrl", () => {
    expect(() =>
      createJazzContext({
        appId: "server-app",
        app: { wasmSchema: SCHEMA_A },
        permissions: {},
        driver: { type: "memory" },
      }),
    ).toThrow("driver.type='memory' requires serverUrl.");
  });
});

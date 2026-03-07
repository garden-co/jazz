import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { AppContext, Session } from "../runtime/context.js";
import { createJazzContext } from "./create-jazz-context.js";

const mocks = vi.hoisted(() => {
  const resolveLocalAuthDefaults = vi.fn();
  const runtimeCtor = vi.fn();
  const inMemoryRuntimeCtor = vi.fn();
  const runtimeInstances: Array<{ flush: ReturnType<typeof vi.fn> }> = [];
  const clients: Array<{
    asBackend: ReturnType<typeof vi.fn>;
    forRequest: ReturnType<typeof vi.fn>;
    forSession: ReturnType<typeof vi.fn>;
    shutdown: ReturnType<typeof vi.fn>;
  }> = [];
  const connectWithRuntime = vi.fn((_runtime: unknown, _context: AppContext) => {
    const client = {
      asBackend: vi.fn(function (this: unknown) {
        return this;
      }),
      forRequest: vi.fn(() => ({ kind: "request-client" })),
      forSession: vi.fn(() => ({ kind: "session-client" })),
      shutdown: vi.fn(async () => undefined),
    };
    clients.push(client);
    return client;
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
    reset() {
      resolveLocalAuthDefaults.mockReset();
      runtimeCtor.mockReset();
      inMemoryRuntimeCtor.mockReset();
      runtimeInstances.length = 0;
      connectWithRuntime.mockClear();
      clients.length = 0;
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

const SCHEMA_A: WasmSchema = {};
const SCHEMA_B: WasmSchema = { todos: { columns: [] } };

describe("backend/create-jazz-context", () => {
  beforeEach(() => {
    mocks.reset();
    mocks.resolveLocalAuthDefaults.mockImplementation((config) => config);
  });

  it("BC-U01: lazily initializes runtime/client on first access", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(mocks.runtimeCtor).not.toHaveBeenCalled();
    expect(mocks.connectWithRuntime).not.toHaveBeenCalled();

    const clientA = context.client();
    const clientB = context.client();

    expect(clientA).toBe(clientB);
    expect(mocks.runtimeCtor).toHaveBeenCalledTimes(1);
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(1);
    expect(mocks.runtimeCtor).toHaveBeenCalledWith(
      serializeRuntimeSchema(SCHEMA_A),
      "server-app",
      "dev",
      "main",
      "/tmp/jazz.db",
      "edge",
    );
  });

  it("BC-U02: supports backend/request/session-scoped helpers", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
      backendSecret: "secret",
    });

    const req = {
      header: (name: string) => (name === "authorization" ? "Bearer a.b.c" : undefined),
    };
    const session: Session = { user_id: "u1", claims: {} };

    const backendClient = context.asBackend();
    const requestClient = context.forRequest(req);
    const sessionClient = context.forSession(session);

    expect(backendClient).toBe(mocks.clients[0]);
    expect(requestClient).toEqual({ kind: "request-client" });
    expect(sessionClient).toEqual({ kind: "session-client" });
    expect(mocks.clients).toHaveLength(1);
    expect(mocks.clients[0].asBackend).toHaveBeenCalledTimes(1);
    expect(mocks.clients[0].forRequest).toHaveBeenCalledWith(req);
    expect(mocks.clients[0].forSession).toHaveBeenCalledWith(session);
  });

  it("BC-U03: throws when no schema source is available", () => {
    const context = createJazzContext({
      appId: "server-app",
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(() => context.client()).toThrow("No schema source provided");
  });

  it("BC-U04: rejects switching to a different schema after initialization", () => {
    const context = createJazzContext({
      appId: "server-app",
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    context.client({ wasmSchema: SCHEMA_A });

    expect(() => context.client({ wasmSchema: SCHEMA_B })).toThrow(
      "already initialized with a different schema",
    );
  });

  it("BC-U05: flush is safe before init and delegates after init", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(() => context.flush()).not.toThrow();
    context.client();
    context.flush();

    expect(mocks.runtimeInstances).toHaveLength(1);
    expect(mocks.runtimeInstances[0].flush).toHaveBeenCalledTimes(1);
  });

  it("BC-U06: shutdown releases client and allows re-init", async () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    context.client();
    expect(mocks.clients).toHaveLength(1);

    await context.shutdown();
    expect(mocks.clients[0].shutdown).toHaveBeenCalledTimes(1);

    context.client();
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(2);
    expect(mocks.runtimeCtor).toHaveBeenCalledTimes(2);
  });

  it("BC-U07: uses in-memory runtime when driver.type is memory", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      driver: { type: "memory" },
      serverUrl: "http://localhost:1625",
    });

    context.client();

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

  it("BC-U08: rejects memory driver without serverUrl", () => {
    expect(() =>
      createJazzContext({
        appId: "server-app",
        app: { wasmSchema: SCHEMA_A },
        driver: { type: "memory" },
      }),
    ).toThrow("driver.type='memory' requires serverUrl.");
  });
});

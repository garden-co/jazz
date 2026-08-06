import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { schema as s } from "../index.js";
import { JazzClient, WriteResult, type InsertResult, type Runtime } from "./client.js";
import { createDbWithRuntimeSource, Db, type DbConfig } from "./db.js";
import {
  RuntimeSource,
  type RuntimeClientContext,
  type RuntimeTokenOptions,
} from "./runtime-source.js";
import type { WasmSchema } from "../drivers/types.js";

class TestDb extends Db {
  static readonly runtime = { TestRuntime: class {} };

  constructor(config: DbConfig, runtimeSource: RuntimeSource<DbConfig> = new TestRuntimeSource()) {
    super(config, runtimeSource);
  }
  public exposeGetClient(schema: WasmSchema): JazzClient {
    return this.getClient(schema);
  }
}

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  protected override async loadRuntime(): Promise<typeof TestDb.runtime> {
    return TestDb.runtime;
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    return JazzClient.connectWithRuntime(
      makeRuntimeStub(),
      {
        appId: config.appId,
        schema,
        driver: config.driver,
        serverUrl: config.serverUrl,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        cookieSession: config.cookieSession,
        adminSecret: config.adminSecret,
        tier: "local",
        defaultDurabilityTier: config.serverUrl ? "edge" : undefined,
      },
      {
        onAuthFailure,
      },
    );
  }
}

function makeSchema(): WasmSchema {
  return {
    todos: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
  };
}

function makeTodosApp() {
  return s.defineApp({
    todos: s.table({
      title: s.string(),
    }),
  });
}

function toBase64Url(value: unknown): string {
  return Buffer.from(JSON.stringify(value), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  return `${toBase64Url({ alg: "HS256", typ: "JWT" })}.${toBase64Url(payload)}.signature`;
}

function makeClientStub() {
  return {
    shutdown: vi.fn(async () => undefined),
    updateAuthToken: vi.fn(),
    connectTransport: vi.fn(),
    disconnectTransport: vi.fn(),
    getRuntime: vi.fn(() => ({}) as never),
  } as unknown as JazzClient & {
    connectTransport: ReturnType<typeof vi.fn>;
    disconnectTransport: ReturnType<typeof vi.fn>;
  };
}

function makeRuntimeStub(): Runtime {
  return {
    insert: vi.fn(),
    restore: vi.fn(),
    update: vi.fn(),
    upsert: vi.fn(),
    delete: vi.fn(),
    beginTransaction: vi.fn(),
    commitTransaction: vi.fn(),
    waitForTransaction: vi.fn(),
    rollbackTransaction: vi.fn(),
    query: vi.fn(),
    createSubscription: vi.fn(),
    executeSubscription: vi.fn(),
    unsubscribe: vi.fn(),
    getSchema: vi.fn(),
    getSchemaHash: vi.fn(),
    connect: vi.fn(),
    disconnect: vi.fn(),
    updateAuth: vi.fn(),
    onAuthFailure: vi.fn(),
  } as unknown as Runtime;
}

describe("runtime/Db native runtime path upstream wiring", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("DBRT-U01 calls connectTransport with serverUrl and derived app scope when configured and no worker", () => {
    const client = makeClientStub();
    const connectWithRuntimeSpy = vi
      .spyOn(JazzClient, "connectWithRuntime")
      .mockReturnValue(client);

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      jwtToken: "jwt-x",
      adminSecret: "admin-y",
    });
    db.exposeGetClient(makeSchema());

    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(1);
    expect((client as any).connectTransport).toHaveBeenCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: "admin-y",
    });
  });

  it("DBRT-U01b calls connectTransport without a separate prefix argument", () => {
    const client = makeClientStub();
    vi.spyOn(JazzClient, "connectWithRuntime").mockReturnValue(client);

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      jwtToken: "jwt-x",
      adminSecret: "admin-y",
    });
    db.exposeGetClient(makeSchema());

    expect((client as any).connectTransport).toHaveBeenCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: "admin-y",
    });
  });

  it("DBRT-U02 does not call connectTransport when serverUrl is absent", () => {
    const client = makeClientStub();
    vi.spyOn(JazzClient, "connectWithRuntime").mockReturnValue(client);

    const db = new TestDb({ appId: "app" });
    db.exposeGetClient(makeSchema());

    expect((client as any).connectTransport).not.toHaveBeenCalled();
  });

  it("DBRT-U02b disconnects and reconnects existing clients through the public Db API", async () => {
    const client = makeClientStub();
    vi.spyOn(JazzClient, "connectWithRuntime").mockReturnValue(client);

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      jwtToken: "jwt-x",
      adminSecret: "admin-y",
    });
    db.exposeGetClient(makeSchema());

    await db.disconnect();
    await db.reconnect();

    expect((client as any).disconnectTransport).toHaveBeenCalledTimes(1);
    expect((client as any).connectTransport).toHaveBeenCalledTimes(2);
    expect((client as any).connectTransport).toHaveBeenLastCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: "admin-y",
      backend_secret: undefined,
      backend_session: undefined,
    });
  });

  it("DBRT-U02c keeps lazily-created clients offline until reconnect", async () => {
    const client = makeClientStub();
    vi.spyOn(JazzClient, "connectWithRuntime").mockReturnValue(client);

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      jwtToken: "jwt-x",
    });

    await db.disconnect();
    db.exposeGetClient(makeSchema());
    expect((client as any).connectTransport).not.toHaveBeenCalled();

    await db.reconnect();
    expect((client as any).connectTransport).toHaveBeenCalledTimes(1);
    expect((client as any).connectTransport).toHaveBeenCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: undefined,
      backend_secret: undefined,
      backend_session: undefined,
    });
  });

  it("DBRT-U03 strips local policies for memory-driver admin-secret clients", () => {
    const client = makeClientStub();
    const connectWithRuntimeSpy = vi
      .spyOn(JazzClient, "connectWithRuntime")
      .mockReturnValue(client);
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
        policies: {
          update: {
            using: {
              type: "False",
            },
          },
        },
      },
    };

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      adminSecret: "admin-y",
      driver: { type: "memory" },
    });
    db.exposeGetClient(schema);

    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(1);
    const runtimeSchema = connectWithRuntimeSpy.mock.calls[0]?.[1].schema;
    expect(runtimeSchema.todos.policies).toBeUndefined();
    expect(runtimeSchema).toMatchObject({
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    });
  });

  it("DBRT-U03b preserves local policies when the runtime source does not support policy bypass", () => {
    const client = makeClientStub();
    const connectWithRuntimeSpy = vi
      .spyOn(JazzClient, "connectWithRuntime")
      .mockReturnValue(client);
    class PolicyEvaluatingRuntimeSource extends TestRuntimeSource {
      override readonly supportsPolicyBypass = false;
    }
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
        policies: {
          update: {
            using: {
              type: "False",
            },
          },
        },
      },
    };

    const db = new TestDb(
      {
        appId: "app",
        serverUrl: "https://example.test",
        adminSecret: "admin-y",
        driver: { type: "memory" },
      },
      new PolicyEvaluatingRuntimeSource(),
    );
    db.exposeGetClient(schema);

    expect(connectWithRuntimeSpy).toHaveBeenCalledTimes(1);
    const runtimeSchema = connectWithRuntimeSpy.mock.calls[0]?.[1].schema;
    expect(runtimeSchema.todos).toHaveProperty("policies", {
      update: {
        using: {
          type: "False",
        },
      },
    });
  });

  it("DBRT-U04 routes Db runtime wiring through an injected runtime source", async () => {
    const app = makeTodosApp();
    const schema = app.todos._schema;
    const loadedRuntime = { kind: "test-core" };
    const runtimeRow: InsertResult = {
      id: "todo-1",
      values: [{ type: "Text", value: "Buy milk" }],
      transactionId: "transaction-1",
    };
    const client = {
      insert: vi.fn(() => new WriteResult(runtimeRow, runtimeRow.transactionId, client)),
      shutdown: vi.fn(async () => undefined),
      updateAuthToken: vi.fn(),
      connectTransport: vi.fn(),
      getRuntime: vi.fn(() => ({}) as never),
    } as unknown as JazzClient & {
      insert: ReturnType<typeof vi.fn>;
      shutdown: ReturnType<typeof vi.fn>;
      updateAuthToken: ReturnType<typeof vi.fn>;
    };
    class TestRuntimeSource extends RuntimeSource<DbConfig> {
      readonly loadRuntimeMock = vi.fn(async (_config: DbConfig) => loadedRuntime);
      override readonly createClient = vi.fn((_context: RuntimeClientContext<DbConfig>) => client);
      override readonly mintLocalFirstToken = vi.fn(
        (options: RuntimeTokenOptions) =>
          `jwt:${options.secret}:${options.audience}:${options.ttlSeconds}`,
      );

      protected override async loadRuntime(config: DbConfig): Promise<typeof loadedRuntime> {
        return await this.loadRuntimeMock(config);
      }
    }
    const runtimeSource = new TestRuntimeSource();

    const db = await createDbWithRuntimeSource(
      {
        appId: "facade-app",
        secret: "alice-secret",
        serverUrl: "https://example.test",
      },
      runtimeSource,
    );

    const inserted = db.insert(app.todos, { title: "Buy milk" });
    db.updateAuthToken("fresh-jwt");
    const proof = db.getLocalFirstIdentityProof({
      audience: "proof-audience",
      ttlSeconds: 7,
    });
    await db.shutdown();

    expect(inserted.value).toEqual({ id: "todo-1", title: "Buy milk" });
    expect(runtimeSource.loadRuntimeMock).toHaveBeenCalledTimes(1);
    expect(runtimeSource.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "facade-app",
        ttlSeconds: 3600,
      }),
    );
    expect(runtimeSource.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        schema,
        config: expect.objectContaining({
          appId: "facade-app",
          jwtToken: "jwt:alice-secret:facade-app:3600",
          serverUrl: "https://example.test",
        }),
        onAuthFailure: expect.any(Function),
      }),
    );
    const createClientContext = runtimeSource.createClient.mock.calls[0]?.[0];
    expect(createClientContext).toBeDefined();
    expect("loadedRuntime" in createClientContext!).toBe(false);
    expect(client.updateAuthToken).toHaveBeenCalledWith("fresh-jwt");
    expect(proof).toBe("jwt:alice-secret:proof-audience:7");
    expect(client.shutdown).toHaveBeenCalledTimes(1);
  });

  it("uses the native runtime path", () => {
    const client = makeClientStub();
    class RecordingRuntimeSource extends RuntimeSource<DbConfig> {
      readonly createClientMock = vi.fn((_context: RuntimeClientContext<DbConfig>) => client);

      protected override async loadRuntime(): Promise<typeof TestDb.runtime> {
        return TestDb.runtime;
      }

      override createClient(context: RuntimeClientContext<DbConfig>): JazzClient {
        return this.createClientMock(context);
      }
    }

    const runtimeSource = new RecordingRuntimeSource();
    new TestDb({ appId: "direct" }, runtimeSource).exposeGetClient(makeSchema());

    expect(runtimeSource.createClientMock.mock.calls[0]?.[0]).toEqual(
      expect.objectContaining({
        config: expect.objectContaining({ appId: "direct" }),
        schema: makeSchema(),
        onAuthFailure: expect.any(Function),
      }),
    );
  });

  it("sends auth refreshes to memoized runtime clients", () => {
    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      jwtToken: makeJwt({ sub: "alice" }),
    });
    const client = db.exposeGetClient(makeSchema());
    const updateAuthToken = vi.spyOn(client, "updateAuthToken");

    const refreshed = makeJwt({ sub: "alice", refresh: 1 });
    db.updateAuthToken(refreshed);

    expect(updateAuthToken).toHaveBeenCalledWith(refreshed);
  });
});

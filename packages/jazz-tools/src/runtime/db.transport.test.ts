import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { schema as s } from "../index.js";
import { JazzClient, WriteResult, type DirectInsertResult } from "./client.js";
import { createDb, Db, type DbConfig } from "./db.js";
import {
  type BackendTokenOptions,
  DbBackendModule,
  type DbBackendClientContext,
} from "./db-backend.js";
import type { WasmSchema } from "../drivers/types.js";

class TestDb extends Db {
  static readonly runtime = { WasmRuntime: class {} };

  constructor(
    config: DbConfig,
    backend: DbBackendModule<DbConfig> = new TestDirectBackendModule(),
  ) {
    super(config, backend);
  }
  public exposeGetClient(schema: WasmSchema): JazzClient {
    return this.getClient(schema);
  }
}

class TestDirectBackendModule extends DbBackendModule<DbConfig> {
  protected override async loadResources(): Promise<typeof TestDb.runtime> {
    return TestDb.runtime;
  }

  override createClient({
    config,
    schema,
    hasWorker,
    useBinaryEncoding,
    onAuthFailure,
    onRejectedBatchAcknowledged,
  }: DbBackendClientContext<DbConfig>): JazzClient {
    return JazzClient.connectSync(
      TestDb.runtime as never,
      {
        appId: config.appId,
        schema,
        driver: config.driver,
        serverUrl: hasWorker ? undefined : config.serverUrl,
        env: config.env,
        userBranch: config.userBranch,
        jwtToken: config.jwtToken,
        cookieSession: config.cookieSession,
        adminSecret: config.adminSecret,
        tier: hasWorker ? undefined : "local",
        defaultDurabilityTier: hasWorker ? undefined : config.serverUrl ? "edge" : undefined,
      },
      {
        useBinaryEncoding,
        onAuthFailure,
        onRejectedBatchAcknowledged,
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

function makeClientStub() {
  return {
    shutdown: vi.fn(async () => undefined),
    updateAuthToken: vi.fn(),
    connectTransport: vi.fn(),
    getRuntime: vi.fn(() => ({}) as never),
  } as unknown as JazzClient & {
    connectTransport: ReturnType<typeof vi.fn>;
  };
}

describe("runtime/Db direct path upstream wiring", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("DBRT-U01 calls connectTransport with serverUrl and derived app scope when configured and no worker", () => {
    const client = makeClientStub();
    const connectSyncSpy = vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);

    const db = new TestDb({
      appId: "app",
      serverUrl: "https://example.test",
      jwtToken: "jwt-x",
      adminSecret: "admin-y",
    });
    db.exposeGetClient(makeSchema());

    expect(connectSyncSpy).toHaveBeenCalledTimes(1);
    expect((client as any).connectTransport).toHaveBeenCalledWith("https://example.test", {
      jwt_token: "jwt-x",
      admin_secret: "admin-y",
    });
  });

  it("DBRT-U01b calls connectTransport without a separate prefix argument", () => {
    const client = makeClientStub();
    vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);

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
    vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);

    const db = new TestDb({ appId: "app" });
    db.exposeGetClient(makeSchema());

    expect((client as any).connectTransport).not.toHaveBeenCalled();
  });

  it("DBRT-U03 strips local policies for memory-driver admin-secret clients", () => {
    const client = makeClientStub();
    const connectSyncSpy = vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);
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

    expect(connectSyncSpy).toHaveBeenCalledTimes(1);
    const runtimeSchema = connectSyncSpy.mock.calls[0]?.[1].schema;
    expect(runtimeSchema.todos.policies).toBeUndefined();
    expect(runtimeSchema).toMatchObject({
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    });
  });

  it("DBRT-U03b preserves local policies when the runtime does not support policy bypass", () => {
    const client = makeClientStub();
    const connectSyncSpy = vi.spyOn(JazzClient, "connectSync").mockReturnValue(client);
    class PolicyEvaluatingBackendModule extends TestDirectBackendModule {
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
      new PolicyEvaluatingBackendModule(),
    );
    db.exposeGetClient(schema);

    expect(connectSyncSpy).toHaveBeenCalledTimes(1);
    const runtimeSchema = connectSyncSpy.mock.calls[0]?.[1].schema;
    expect(runtimeSchema.todos).toHaveProperty("policies", {
      update: {
        using: {
          type: "False",
        },
      },
    });
  });

  it("DBRT-U04 routes Db client wiring through an injected backend module", async () => {
    const app = makeTodosApp();
    const schema = app.todos._schema;
    const loadedResources = { kind: "test-backend-resources" };
    const runtimeRow: DirectInsertResult = {
      id: "todo-1",
      values: [{ type: "Text", value: "Buy milk" }],
      batchId: "batch-1",
    };
    const client = {
      create: vi.fn(() => new WriteResult(runtimeRow, runtimeRow.batchId, client)),
      shutdown: vi.fn(async () => undefined),
      updateAuthToken: vi.fn(),
      connectTransport: vi.fn(),
      getRuntime: vi.fn(() => ({}) as never),
    } as unknown as JazzClient & {
      create: ReturnType<typeof vi.fn>;
      shutdown: ReturnType<typeof vi.fn>;
      updateAuthToken: ReturnType<typeof vi.fn>;
    };
    class TestBackendModule extends DbBackendModule<DbConfig> {
      readonly loadResourcesMock = vi.fn(async (_config: DbConfig) => loadedResources);
      override readonly createClient = vi.fn(
        (_context: DbBackendClientContext<DbConfig>) => client,
      );
      override readonly mintLocalFirstToken = vi.fn(
        (options: BackendTokenOptions) =>
          `jwt:${options.secret}:${options.audience}:${options.ttlSeconds}`,
      );

      protected override async loadResources(config: DbConfig): Promise<typeof loadedResources> {
        return await this.loadResourcesMock(config);
      }
    }
    const runtime = new TestBackendModule();

    const db = await createDb({
      appId: "facade-app",
      secret: "alice-secret",
      serverUrl: "https://example.test",
      runtime,
    });

    const inserted = db.insert(app.todos, { title: "Buy milk" });
    db.updateAuthToken("fresh-jwt");
    const proof = db.getLocalFirstIdentityProof({
      audience: "proof-audience",
      ttlSeconds: 7,
    });
    await db.shutdown();

    expect(inserted.value).toEqual({ id: "todo-1", title: "Buy milk" });
    expect(runtime.loadResourcesMock).toHaveBeenCalledTimes(1);
    expect(runtime.mintLocalFirstToken).toHaveBeenCalledWith(
      expect.objectContaining({
        secret: "alice-secret",
        audience: "facade-app",
        ttlSeconds: 3600,
      }),
    );
    expect(runtime.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        schema,
        hasWorker: false,
        useBinaryEncoding: false,
        config: expect.objectContaining({
          appId: "facade-app",
          jwtToken: "jwt:alice-secret:facade-app:3600",
          serverUrl: "https://example.test",
        }),
        onAuthFailure: expect.any(Function),
        onRejectedBatchAcknowledged: expect.any(Function),
      }),
    );
    const createClientContext = runtime.createClient.mock.calls[0]?.[0];
    expect(createClientContext).toBeDefined();
    expect("loadedResources" in createClientContext!).toBe(false);
    expect(client.updateAuthToken).toHaveBeenCalledWith("fresh-jwt");
    expect(proof).toBe("jwt:alice-secret:proof-audience:7");
    expect(client.shutdown).toHaveBeenCalledTimes(1);
  });
});

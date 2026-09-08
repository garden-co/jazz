import { accountRegistryUrl } from "../accounts/context.js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { CompiledPermissions } from "../permissions/index.js";
import type { AppContext, Session } from "../runtime/context.js";
import { createJazzContext } from "./create-jazz-context.js";

const mocks = vi.hoisted(() => {
  const resolveRequestSession = vi.fn();
  const openMemory = vi.fn();
  const openMemoryAsBackend = vi.fn();
  const openPersistent = vi.fn();
  const openPersistentAsBackend = vi.fn();
  const nativeRuntimeCtor = vi.fn();
  const runtimeInstances: Array<{ close: ReturnType<typeof vi.fn> }> = [];
  const clients: Array<{
    asBackend: ReturnType<typeof vi.fn>;
    connectTransport: ReturnType<typeof vi.fn>;
    shutdown: ReturnType<typeof vi.fn>;
  }> = [];
  const connectWithRuntime = vi.fn((_runtime: unknown, _context: AppContext) => {
    const client = {
      asBackend: vi.fn(function (this: unknown) {
        return this;
      }),
      connectTransport: vi.fn(),
      shutdown: vi.fn(async () => undefined),
    };
    clients.push(client);
    return client;
  });
  const fakeDb = {
    close: vi.fn(),
  };

  const MockNapiDb = {
    openMemory: vi.fn((schemaBytes: Uint8Array, configBytes: Uint8Array) => {
      openMemory(schemaBytes, configBytes);
      return fakeDb;
    }),
    openMemoryAsBackend: vi.fn((schemaBytes: Uint8Array, configBytes: Uint8Array) => {
      openMemoryAsBackend(schemaBytes, configBytes);
      return fakeDb;
    }),
    openPersistent: vi.fn((dataPath: string, schemaBytes: Uint8Array, configBytes: Uint8Array) => {
      openPersistent(dataPath, schemaBytes, configBytes);
      return fakeDb;
    }),
    openPersistentAsBackend: vi.fn(
      (dataPath: string, schemaBytes: Uint8Array, configBytes: Uint8Array) => {
        openPersistentAsBackend(dataPath, schemaBytes, configBytes);
        return fakeDb;
      },
    ),
  };

  class MockNativeRuntimeAdapter {
    readonly close = vi.fn();
    constructor(
      Runtime: typeof MockNapiDb,
      schema: WasmSchema,
      node: Uint8Array,
      author: Uint8Array,
      sourceId: number,
      historyComplete: boolean,
      opts?: { persistentPath?: string; backendMode?: boolean },
    ) {
      nativeRuntimeCtor(Runtime, schema, node, author, sourceId, historyComplete, opts);
      if (opts?.persistentPath && opts.backendMode) {
        Runtime.openPersistentAsBackend(
          opts.persistentPath,
          new TextEncoder().encode(JSON.stringify(schema)),
          new Uint8Array(),
        );
      } else if (opts?.persistentPath) {
        Runtime.openPersistent(
          opts.persistentPath,
          new TextEncoder().encode(JSON.stringify(schema)),
          new Uint8Array(),
        );
      } else if (opts?.backendMode) {
        Runtime.openMemoryAsBackend(
          new TextEncoder().encode(JSON.stringify(schema)),
          new Uint8Array(),
        );
      } else {
        Runtime.openMemory(new TextEncoder().encode(JSON.stringify(schema)), new Uint8Array());
      }
      runtimeInstances.push(this);
    }
  }

  class MockJazzClient {
    static connectWithRuntime = connectWithRuntime;
  }

  return {
    MockNapiDb,
    MockNativeRuntimeAdapter,
    MockJazzClient,
    resolveRequestSession,
    openMemory,
    openMemoryAsBackend,
    openPersistent,
    openPersistentAsBackend,
    nativeRuntimeCtor,
    runtimeInstances,
    connectWithRuntime,
    clients,
    reset() {
      resolveRequestSession.mockReset();
      openMemory.mockReset();
      openMemoryAsBackend.mockReset();
      openPersistent.mockReset();
      openPersistentAsBackend.mockReset();
      nativeRuntimeCtor.mockReset();
      MockNapiDb.openMemory.mockClear();
      MockNapiDb.openMemoryAsBackend.mockClear();
      MockNapiDb.openPersistent.mockClear();
      MockNapiDb.openPersistentAsBackend.mockClear();
      fakeDb.close.mockClear();
      runtimeInstances.length = 0;
      connectWithRuntime.mockClear();
      clients.length = 0;
    },
  };
});

vi.mock("jazz-napi", () => ({
  NapiDb: mocks.MockNapiDb,
}));

vi.mock("../runtime/native-runtime/native-runtime-adapter.js", () => ({
  NativeRuntimeAdapter: mocks.MockNativeRuntimeAdapter,
}));

vi.mock("../runtime/client.js", async () => {
  const actual = await vi.importActual("../runtime/client.js");
  return {
    ...actual,
    JazzClient: mocks.MockJazzClient,
  };
});

vi.mock("./request-auth.js", () => ({
  resolveRequestSession: mocks.resolveRequestSession,
  verifiedLocalFirstRequestProof: () => undefined,
}));

const SCHEMA_A: WasmSchema = {};
const SCHEMA_B: WasmSchema = { todos: { columns: [] } };
const TODO_PERMISSIONS: CompiledPermissions = {
  todos: {
    select: { using: { type: "True" } },
  },
};
const RELATION_LITERAL_PERMISSIONS: CompiledPermissions = {
  resources: {
    select: {
      using: {
        type: "ExistsRel",
        rel: {
          Filter: {
            input: {
              TableScan: {
                table: "resource_access_edges",
              },
            },
            predicate: {
              And: [
                {
                  Cmp: {
                    left: {
                      scope: "resource_access_edges",
                      column: "kind",
                    },
                    op: "Eq",
                    right: {
                      Literal: "individual",
                    },
                  },
                },
                {
                  Cmp: {
                    left: {
                      scope: "resource_access_edges",
                      column: "grant_role",
                    },
                    op: "Eq",
                    right: {
                      Literal: "viewer",
                    },
                  },
                },
              ],
            },
          },
        },
      },
    },
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
    mocks.resolveRequestSession.mockResolvedValue({
      issuer: "https://issuer.example",
      user_id: "u1",
      claims: {},
      authMode: "external" as const,
    });
  });

  it("rejects unassigned requests before opening a runtime", async () => {
    const context = createJazzContext({
      appId: "server-app",
      serverUrl: "https://core.example",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "memory" },
    });
    mocks.resolveRequestSession.mockRejectedValue(new Error("identity_not_assigned"));
    await expect(
      context.forRequest({ headers: { authorization: "Bearer rejected" } }),
    ).rejects.toThrow("identity_not_assigned");
    expect(mocks.nativeRuntimeCtor).not.toHaveBeenCalled();
    expect(mocks.connectWithRuntime).not.toHaveBeenCalled();
  });

  it("BC-U01: lazily initializes runtime/client on first access", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(mocks.nativeRuntimeCtor).not.toHaveBeenCalled();
    expect(mocks.connectWithRuntime).not.toHaveBeenCalled();

    const dbA = context.db();
    const dbB = context.db();

    expect(dbA).not.toBe(dbB);
    expect(mocks.nativeRuntimeCtor).toHaveBeenCalledTimes(1);
    expect(mocks.openPersistentAsBackend).toHaveBeenCalledTimes(1);
    expect(mocks.openMemoryAsBackend).not.toHaveBeenCalled();
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(1);
    expect(mocks.nativeRuntimeCtor).toHaveBeenCalledWith(
      mocks.MockNapiDb,
      SCHEMA_A,
      expect.any(Uint8Array),
      expect.any(Uint8Array),
      1,
      true,
      {
        persistentPath: "/tmp/jazz.db",
        readAuthorizationHost: "trusted-serving",
        backendMode: true,
      },
    );
  });

  it("BC-U01a: respects an explicit default durability tier over the node tier", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
      tier: "local",
      defaultDurabilityTier: "edge",
    });

    context.db();

    expect(mocks.connectWithRuntime).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        tier: "local",
        defaultDurabilityTier: "edge",
      }),
      expect.anything(),
    );
  });

  it("BC-U01aa: defaults a connected backend context to its node tier", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
      serverUrl: "http://localhost:1625",
      tier: "global",
    });

    context.db();

    expect(mocks.connectWithRuntime).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        tier: "global",
        defaultDurabilityTier: "global",
      }),
      expect.anything(),
    );
  });

  it("BC-U01b: rejects configuring both jwksUrl and jwtPublicKey", () => {
    expect(() =>
      createJazzContext({
        appId: "server-app",
        app: { wasmSchema: SCHEMA_A },
        permissions: {},
        driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
        jwksUrl: "https://issuer.example/.well-known/jwks.json",
        jwtPublicKey: {
          kty: "oct",
          kid: "static-kid",
          alg: "HS256",
          k: "c3RhdGljLXNlY3JldA",
        },
      }),
    ).toThrow(/jwksUrl.*jwtPublicKey|jwtPublicKey.*jwksUrl/i);
  });

  it("BC-U02: supports high-level db/backend/request/session/attribution helpers", async () => {
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
    const session: Session = {
      issuer: "https://issuer.example",
      user_id: "u1",
      claims: {},
      authMode: "external",
    };

    const db = context.db();
    const backendDb = context.asBackend();
    const requestDb = await context.forRequest(req);
    const sessionDb = context.forSession(session);
    const attributedDb = context.withAttribution("https://issuer.example", "u2");
    const attributedSessionDb = context.withAttributionForSession(session);
    const attributedRequestDb = await context.withAttributionForRequest(req);

    // An un-attributed backend read deliberately passes no logical session:
    // it is trusted serving, not a public request impersonating SYSTEM. A
    // request keeps its external session and therefore remains policy-scoped.
    expect((backendDb as any).getRuntimeOperationContext()).toBeNull();
    expect((requestDb as any).getRuntimeOperationContext()).toMatchObject({
      session,
      readSession: undefined,
    });

    for (const scopedDb of [
      db,
      backendDb,
      requestDb,
      sessionDb,
      attributedDb,
      attributedSessionDb,
      attributedRequestDb,
    ]) {
      expect(scopedDb).toHaveProperty("getAuthState");
    }
    expect(requestDb.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: { account: null, identity: { issuer: session.issuer, subject: session.user_id } },
      },
    });
    expect(sessionDb.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: { account: null, identity: { issuer: session.issuer, subject: session.user_id } },
      },
    });
    expect(mocks.resolveRequestSession).toHaveBeenCalledTimes(2);
    expect(mocks.resolveRequestSession).toHaveBeenNthCalledWith(
      1,
      req,
      {
        appId: "server-app",
        accountRegistry: accountRegistryUrl("http://localhost:1625", "server-app"),
        jwksUrl: undefined,
        allowLocalFirstAuth: true,
        jwtPublicKey: undefined,
        jwtIssuer: undefined,
        jwtAudience: undefined,
      },
      undefined,
    );
    expect(mocks.resolveRequestSession).toHaveBeenNthCalledWith(
      2,
      req,
      {
        appId: "server-app",
        accountRegistry: accountRegistryUrl("http://localhost:1625", "server-app"),
        jwksUrl: undefined,
        allowLocalFirstAuth: true,
        jwtPublicKey: undefined,
        jwtIssuer: undefined,
        jwtAudience: undefined,
      },
      undefined,
    );
    expect(mocks.clients).toHaveLength(1);
    expect(mocks.clients[0]!.asBackend).toHaveBeenCalledTimes(6);
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(1);
  });

  it("BC-U03: local-only backends require explicit trusted sessions instead of public request admission", async () => {
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
    const session: Session = {
      issuer: "https://issuer.example",
      user_id: "u1",
      claims: {},
      authMode: "external",
    };

    await expect(context.forRequest(req)).rejects.toThrow(/core serverUrl for account admission/);
    expect(() => context.forSession(session)).not.toThrow();
    expect(() => context.withAttribution("https://issuer.example", "u2")).not.toThrow();
    expect(() => context.withAttributionForSession(session)).not.toThrow();
    await expect(context.withAttributionForRequest(req)).rejects.toThrow(
      /core serverUrl for account admission/,
    );
    expect(mocks.clients[0]!.asBackend).not.toHaveBeenCalled();
  });

  it("BC-U03b: forwards backend request auth config into request session resolution", async () => {
    const context = createJazzContext({
      appId: "server-app",
      serverUrl: "http://localhost:1625",
      backendSecret: "secret",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
      jwksUrl: "https://issuer.example/.well-known/jwks.json",
      allowLocalFirstAuth: false,
    });

    const req = {
      header: (name: string) =>
        name === "authorization" ? `Bearer ${makeJwt({ sub: "u1" })}` : undefined,
    };

    await context.forRequest(req);

    expect(mocks.resolveRequestSession).toHaveBeenCalledWith(
      req,
      {
        appId: "server-app",
        accountRegistry: accountRegistryUrl("http://localhost:1625", "server-app"),
        jwksUrl: "https://issuer.example/.well-known/jwks.json",
        allowLocalFirstAuth: false,
        jwtPublicKey: undefined,
        jwtIssuer: undefined,
        jwtAudience: undefined,
      },
      undefined,
    );
  });

  it("BC-U03c: forwards jwtPublicKey into request session resolution", async () => {
    const context = createJazzContext({
      appId: "server-app",
      serverUrl: "http://localhost:1625",
      backendSecret: "secret",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
      jwtPublicKey: {
        kty: "oct",
        kid: "static-kid",
        alg: "HS256",
        k: "c3RhdGljLXNlY3JldA",
      },
      allowLocalFirstAuth: false,
    });

    const req = {
      header: (name: string) =>
        name === "authorization" ? `Bearer ${makeJwt({ sub: "u1" })}` : undefined,
    };

    await context.forRequest(req);

    expect(mocks.resolveRequestSession).toHaveBeenCalledWith(
      req,
      {
        appId: "server-app",
        accountRegistry: accountRegistryUrl("http://localhost:1625", "server-app"),
        jwksUrl: undefined,
        jwtPublicKey: {
          kty: "oct",
          kid: "static-kid",
          alg: "HS256",
          k: "c3RhdGljLXNlY3JldA",
        },
        allowLocalFirstAuth: false,
        jwtIssuer: undefined,
        jwtAudience: undefined,
      },
      undefined,
    );
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

    expect(mocks.nativeRuntimeCtor.mock.calls[0]![1]).toEqual({
      todos: {
        columns: [],
        policies: TODO_PERMISSIONS.todos as any,
      },
    });
  });

  it("BC-U04b: normalizes relation literals before serializing runtime schema JSON", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: {
        wasmSchema: {
          resources: {
            columns: [],
          },
        },
      },
      permissions: RELATION_LITERAL_PERMISSIONS,
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    context.db();

    expect(mocks.nativeRuntimeCtor.mock.calls[0]![1]).toEqual({
      resources: {
        columns: [],
        policies: {
          select: {
            using: {
              type: "ExistsRel",
              rel: {
                Filter: {
                  input: {
                    TableScan: {
                      table: "resource_access_edges",
                    },
                  },
                  predicate: {
                    And: [
                      {
                        Cmp: {
                          left: {
                            scope: "resource_access_edges",
                            column: "kind",
                          },
                          op: "Eq",
                          right: {
                            Literal: {
                              type: "Text",
                              value: "individual",
                            },
                          },
                        },
                      },
                      {
                        Cmp: {
                          left: {
                            scope: "resource_access_edges",
                            column: "grant_role",
                          },
                          op: "Eq",
                          right: {
                            Literal: {
                              type: "Text",
                              value: "viewer",
                            },
                          },
                        },
                      },
                    ],
                  },
                },
              },
            },
          },
        },
      },
    });
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

  it("BC-U06: flush is safe before and after init", () => {
    const context = createJazzContext({
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent", dataPath: "/tmp/jazz.db" },
    });

    expect(() => context.flush()).not.toThrow();
    context.db();

    expect(mocks.runtimeInstances).toHaveLength(1);
    expect(() => context.flush()).not.toThrow();
  });

  it("BC-U07: shutdown is terminal and a new context creates a new runtime", async () => {
    const config = {
      appId: "server-app",
      app: { wasmSchema: SCHEMA_A },
      permissions: {},
      driver: { type: "persistent" as const, dataPath: "/tmp/jazz.db" },
    };
    const context = createJazzContext(config);

    context.db();
    expect(mocks.clients).toHaveLength(1);

    await context.shutdown();
    await context.shutdown();
    expect(mocks.clients[0]!.shutdown).toHaveBeenCalledTimes(1);
    expect(() => context.db()).toThrow("JazzContext is shutting down or has already shut down.");
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(1);
    expect(mocks.nativeRuntimeCtor).toHaveBeenCalledTimes(1);

    createJazzContext(config).db();
    expect(mocks.connectWithRuntime).toHaveBeenCalledTimes(2);
    expect(mocks.nativeRuntimeCtor).toHaveBeenCalledTimes(2);
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

    expect(mocks.openMemoryAsBackend).toHaveBeenCalledTimes(1);
    expect(mocks.openPersistentAsBackend).not.toHaveBeenCalled();
    expect(mocks.nativeRuntimeCtor).toHaveBeenCalledWith(
      mocks.MockNapiDb,
      SCHEMA_A,
      expect.any(Uint8Array),
      expect.any(Uint8Array),
      1,
      true,
      { readAuthorizationHost: "trusted-serving", backendMode: true },
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

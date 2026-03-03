import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext } from "./context.js";

const schemaWithTodos = {
  todos: {
    columns: [
      {
        name: "done",
        column_type: { type: "Boolean" as const },
        nullable: false,
      },
    ],
  },
} as AppContext["schema"];

function toBase64Url(value: unknown): string {
  const encoded = Buffer.from(JSON.stringify(value), "utf8").toString("base64");
  return encoded.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function makeJwt(payload: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  return `${toBase64Url(header)}.${toBase64Url(payload)}.signature`;
}

function makeClient() {
  const queryCalls: Array<[string, string | undefined, string | undefined, string | undefined]> =
    [];
  const subscribeCalls: Array<
    [string, string | undefined, string | undefined, string | undefined]
  > = [];
  const unsubscribeCalls: number[] = [];
  const subscribeCallbacks: Array<Function> = [];

  const runtime: Runtime = {
    insert: () => "00000000-0000-0000-0000-000000000001",
    update: () => {},
    delete: () => {},
    query: async (
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ) => {
      queryCalls.push([
        queryJson,
        sessionJson ?? undefined,
        tier ?? undefined,
        optionsJson ?? undefined,
      ]);
      return [];
    },
    subscribe: (
      queryJson: string,
      onUpdate: Function,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ) => {
      subscribeCalls.push([
        queryJson,
        sessionJson ?? undefined,
        tier ?? undefined,
        optionsJson ?? undefined,
      ]);
      subscribeCallbacks.push(onUpdate);
      return 1;
    },
    unsubscribe: (handle: number) => {
      unsubscribeCalls.push(handle);
    },
    insertDurable: async () => "00000000-0000-0000-0000-000000000001",
    updateDurable: async () => {},
    deleteDurable: async () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const context: AppContext = {
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
    backendSecret: "test-backend-secret",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "worker" | "edge" | "global",
    ): JazzClient;
  };
  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    queryCalls,
    subscribeCalls,
    unsubscribeCalls,
    subscribeCallbacks,
  };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
}

function makeClientWithContext(context: AppContext): JazzClient {
  const runtime: Runtime = {
    insert: () => "00000000-0000-0000-0000-000000000001",
    update: () => {},
    delete: () => {},
    query: async () => [],
    subscribe: () => 1,
    unsubscribe: () => {},
    insertDurable: async () => "00000000-0000-0000-0000-000000000001",
    updateDurable: async () => {},
    deleteDurable: async () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "worker" | "edge" | "global",
    ): JazzClient;
  };
  return new JazzClientCtor(runtime, context, "edge");
}

describe("JazzClient.forRequest", () => {
  it("enables backend mode when backend secret + server URL are configured", () => {
    const { client } = makeClient();
    expect(client.asBackend()).toBe(client);
  });

  it("throws when backend mode is requested without backend secret", () => {
    const client = makeClientWithContext({
      appId: "test-app",
      schema: {},
      serverUrl: "http://localhost:1625",
    });
    expect(() => client.asBackend()).toThrow("backendSecret required for backend mode");
  });

  it("throws when backend mode is requested without server URL", () => {
    const client = makeClientWithContext({
      appId: "test-app",
      schema: {},
      backendSecret: "test-backend-secret",
    });
    expect(() => client.asBackend()).toThrow("serverUrl required for backend mode");
  });

  it("extracts sub + claims from a bearer JWT", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({
      sub: "user-123",
      claims: { role: "admin" },
    });

    const scopedClient = client.forRequest({
      header(name: string) {
        return name.toLowerCase() === "authorization" ? `Bearer ${token}` : undefined;
      },
    });

    await scopedClient.query('{"table":"todos"}');

    expect(queryCalls.length).toBe(1);
    expect(queryCalls[0][1]).toBe(
      JSON.stringify({
        user_id: "user-123",
        claims: { role: "admin" },
      }),
    );
  });

  it("supports Node-style headers object", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-456" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: [`Bearer ${token}`],
      },
    });

    await scopedClient.query('{"table":"todos"}');

    expect(queryCalls[0][1]).toBe(
      JSON.stringify({
        user_id: "user-456",
        claims: {},
      }),
    );
  });

  it("throws when Authorization header is missing", () => {
    const { client } = makeClient();

    expect(() => client.forRequest({ headers: {} })).toThrow(
      "Missing or invalid Authorization header",
    );
  });

  it("throws when JWT sub is missing", () => {
    const { client } = makeClient();
    const token = makeJwt({ claims: { role: "admin" } });

    expect(() =>
      client.forRequest({
        headers: {
          authorization: `Bearer ${token}`,
        },
      }),
    ).toThrow("JWT payload missing sub");
  });

  it("accepts query builders for session-scoped query calls", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-789" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });

    const builder = {
      _build() {
        return '{"table":"todos","conditions":[{"column":"done","op":"eq","value":true}]}';
      },
    };

    await scopedClient.query(builder);

    expect(queryCalls[0][0]).toBe(builder._build());
  });

  it("translates schema-aware query builders for session-scoped query calls", async () => {
    const { client, queryCalls } = makeClient();
    const token = makeJwt({ sub: "user-901" });

    const scopedClient = client.forRequest({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });

    const builder = {
      _schema: schemaWithTodos,
      _build() {
        return JSON.stringify({
          table: "todos",
          conditions: [{ column: "done", op: "eq", value: true }],
          includes: {},
          orderBy: [],
        });
      },
    };

    await scopedClient.query(builder);

    const parsed = JSON.parse(queryCalls[0][0]) as Record<string, unknown>;
    expect(parsed.table).toBe("todos");
    expect(parsed).toHaveProperty("relation_ir");
  });

  it("accepts query builders for subscribe calls", async () => {
    const { client, subscribeCalls } = makeClient();

    const builder = {
      _build() {
        return '{"table":"todos"}';
      },
    };

    const subId = client.subscribe(builder, () => {});

    expect(subId).toBe(1);
    expect(subscribeCalls).toHaveLength(0);
    await flushMicrotasks();
    expect(subscribeCalls[0][0]).toBe(builder._build());
  });

  it("translates schema-aware query builders for subscribe calls", async () => {
    const { client, subscribeCalls } = makeClient();

    const builder = {
      _schema: schemaWithTodos,
      _build() {
        return JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        });
      },
    };

    const subId = client.subscribe(builder, () => {});

    expect(subId).toBe(1);
    expect(subscribeCalls).toHaveLength(0);
    await flushMicrotasks();
    const parsed = JSON.parse(subscribeCalls[0][0]) as Record<string, unknown>;
    expect(parsed.table).toBe("todos");
    expect(parsed).toHaveProperty("relation_ir");
  });

  it("forwards structured RN delta payloads to subscription callbacks", async () => {
    const { client, subscribeCallbacks } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    subscribeCallbacks[0](
      JSON.stringify({
        added: [{ row: { id: "row-a", values: [] }, index: 0 }],
        removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
        updated: [
          {
            old_row: { id: "row-u", values: [] },
            new_row: { id: "row-u", values: [] },
            old_index: 0,
            new_index: 0,
          },
        ],
        pending: false,
      }),
    );

    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith({
      added: [{ row: { id: "row-a", values: [] }, index: 0 }],
      removed: [{ row: { id: "row-r", values: [] }, index: 1 }],
      updated: [
        {
          old_row: { id: "row-u", values: [] },
          new_row: { id: "row-u", values: [] },
          old_index: 0,
          new_index: 0,
        },
      ],
      pending: false,
    });
  });

  it("forwards partial structured deltas without throwing", async () => {
    const { client, subscribeCallbacks } = makeClient();
    const callback = vi.fn();
    client.subscribe('{"table":"todos"}', callback);
    await flushMicrotasks();

    expect(() =>
      subscribeCallbacks[0](
        JSON.stringify({
          pending: true,
        }),
      ),
    ).not.toThrow();

    expect(callback).toHaveBeenCalledWith({
      pending: true,
    });
  });

  it("passes query propagation options to runtime query", async () => {
    const { client, queryCalls } = makeClient();
    await client.query('{"table":"todos"}', { propagation: "local-only" });
    expect(queryCalls[0][3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  it("passes query propagation options to runtime subscribe", async () => {
    const { client, subscribeCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {}, { propagation: "local-only" });
    await flushMicrotasks();
    expect(subscribeCalls[0][3]).toBe(JSON.stringify({ propagation: "local-only" }));
  });

  it("returns provisional subscription handle synchronously", () => {
    const { client, subscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    expect(subId).toBe(1);
    expect(subscribeCalls).toHaveLength(0);
  });

  it("defers runtime subscribe to a microtask", async () => {
    const { client, subscribeCalls } = makeClient();
    client.subscribe('{"table":"todos"}', () => {});
    expect(subscribeCalls).toHaveLength(0);
    await flushMicrotasks();
    expect(subscribeCalls).toHaveLength(1);
  });

  it("cancel-before-bind: unsubscribe before microtask prevents runtime subscribe", async () => {
    const { client, subscribeCalls, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    client.unsubscribe(subId);
    await flushMicrotasks();
    expect(subscribeCalls).toHaveLength(0);
    expect(unsubscribeCalls).toHaveLength(0);
  });

  it("unsubscribe after bind unsubscribes runtime handle", async () => {
    const { client, unsubscribeCalls } = makeClient();
    const subId = client.subscribe('{"table":"todos"}', () => {});
    await flushMicrotasks();
    client.unsubscribe(subId);
    expect(unsubscribeCalls).toEqual([1]);
  });

  it("unsubscribe unknown handle is a no-op", () => {
    const { client } = makeClient();
    expect(() => client.unsubscribe(123_456)).not.toThrow();
  });

  it("subscribe bind failure cleans up subscription state", async () => {
    const runtime: Runtime = {
      insert: () => "00000000-0000-0000-0000-000000000001",
      update: () => {},
      delete: () => {},
      query: async () => [],
      subscribe: () => {
        throw new Error("subscribe failed");
      },
      unsubscribe: () => {},
      insertDurable: async () => "00000000-0000-0000-0000-000000000001",
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "00000000-0000-0000-0000-000000000001",
      getSchema: () => ({}),
      getSchemaHash: () => "schema-hash",
    };
    const context: AppContext = {
      appId: "test-app",
      schema: {},
      serverUrl: "http://localhost:1625",
      backendSecret: "test-backend-secret",
    };
    const JazzClientCtor = JazzClient as unknown as {
      new (
        runtime: Runtime,
        context: AppContext,
        defaultDurabilityTier: "worker" | "edge" | "global",
      ): JazzClient;
    };
    const client = new JazzClientCtor(runtime, context, "edge");

    const subId = client.subscribe('{"table":"todos"}', () => {});
    await flushMicrotasks();

    // Should not throw after failed bind; state must already be cleaned up.
    expect(() => client.unsubscribe(subId)).not.toThrow();
  });

  it("uses runtime-provided scheduler when available", () => {
    const scheduleCalls: Array<() => void> = [];
    const subscribeCalls: string[] = [];
    const runtime: Runtime = {
      schedule: (task) => {
        scheduleCalls.push(task);
      },
      insert: () => "00000000-0000-0000-0000-000000000001",
      update: () => {},
      delete: () => {},
      query: async () => [],
      subscribe: (queryJson: string) => {
        subscribeCalls.push(queryJson);
        return 1;
      },
      unsubscribe: () => {},
      insertDurable: async () => "00000000-0000-0000-0000-000000000001",
      updateDurable: async () => {},
      deleteDurable: async () => {},
      onSyncMessageReceived: () => {},
      onSyncMessageToSend: () => {},
      addServer: () => {},
      removeServer: () => {},
      addClient: () => "00000000-0000-0000-0000-000000000001",
      getSchema: () => ({}),
      getSchemaHash: () => "schema-hash",
    };
    const context: AppContext = {
      appId: "test-app",
      schema: {},
      serverUrl: "http://localhost:1625",
      backendSecret: "test-backend-secret",
    };
    const JazzClientCtor = JazzClient as unknown as {
      new (
        runtime: Runtime,
        context: AppContext,
        defaultDurabilityTier: "worker" | "edge" | "global",
      ): JazzClient;
    };
    const client = new JazzClientCtor(runtime, context, "edge");

    client.subscribe('{"table":"todos"}', () => {});
    expect(scheduleCalls).toHaveLength(1);
    expect(subscribeCalls).toHaveLength(0);

    scheduleCalls[0]();
    expect(subscribeCalls).toEqual(['{"table":"todos"}']);
  });

  it("browser default scheduler uses postTask with user-visible priority", () => {
    const priorWindow = (globalThis as { window?: unknown }).window;
    const priorDocument = (globalThis as { document?: unknown }).document;
    const priorScheduler = (globalThis as { scheduler?: unknown }).scheduler;

    const postTask = vi.fn((task: () => void) => {
      task();
      return Promise.resolve();
    });

    Object.defineProperty(globalThis, "window", { value: {}, configurable: true });
    Object.defineProperty(globalThis, "document", { value: {}, configurable: true });
    Object.defineProperty(globalThis, "scheduler", { value: { postTask }, configurable: true });

    try {
      const subscribeCalls: string[] = [];
      const runtime: Runtime = {
        insert: () => "00000000-0000-0000-0000-000000000001",
        update: () => {},
        delete: () => {},
        query: async () => [],
        subscribe: (queryJson: string) => {
          subscribeCalls.push(queryJson);
          return 1;
        },
        unsubscribe: () => {},
        insertDurable: async () => "00000000-0000-0000-0000-000000000001",
        updateDurable: async () => {},
        deleteDurable: async () => {},
        onSyncMessageReceived: () => {},
        onSyncMessageToSend: () => {},
        addServer: () => {},
        removeServer: () => {},
        addClient: () => "00000000-0000-0000-0000-000000000001",
        getSchema: () => ({}),
        getSchemaHash: () => "schema-hash",
      };
      const context: AppContext = {
        appId: "test-app",
        schema: {},
        serverUrl: "http://localhost:1625",
        backendSecret: "test-backend-secret",
      };
      const JazzClientCtor = JazzClient as unknown as {
        new (
          runtime: Runtime,
          context: AppContext,
          defaultDurabilityTier: "worker" | "edge" | "global",
        ): JazzClient;
      };
      const client = new JazzClientCtor(runtime, context, "edge");

      client.subscribe('{"table":"todos"}', () => {});

      expect(postTask).toHaveBeenCalledTimes(1);
      expect(postTask).toHaveBeenCalledWith(expect.any(Function), { priority: "user-visible" });
      expect(subscribeCalls).toEqual(['{"table":"todos"}']);
    } finally {
      if (priorWindow === undefined) {
        delete (globalThis as { window?: unknown }).window;
      } else {
        Object.defineProperty(globalThis, "window", { value: priorWindow, configurable: true });
      }
      if (priorDocument === undefined) {
        delete (globalThis as { document?: unknown }).document;
      } else {
        Object.defineProperty(globalThis, "document", { value: priorDocument, configurable: true });
      }
      if (priorScheduler === undefined) {
        delete (globalThis as { scheduler?: unknown }).scheduler;
      } else {
        Object.defineProperty(globalThis, "scheduler", {
          value: priorScheduler,
          configurable: true,
        });
      }
    }
  });
});

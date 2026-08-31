import { describe, it, expect, vi } from "vitest";
import {
  JazzClient,
  ExclusiveWriteHandle,
  ReadTier,
  resolveDefaultDurabilityTier,
  resolveEffectiveQueryExecutionOptions,
  resolveReadTier,
  type Runtime,
  type TransactionalRuntime,
  type TxId,
  type MutationErrorEvent,
  type OpenTransactionId,
  type WriteReceipt,
} from "./client.js";
import type { AppContext } from "./context.js";
import type { RuntimeSubscriptionDelta, WasmSchema } from "../drivers/types.js";

function makeFakeRuntime() {
  let mutationErrorCallback: ((event: MutationErrorEvent) => void) | null = null;
  let nextTransactionNumber = 0;

  function openTransactionIdFromWriteContext(
    writeContextJson?: string | null,
  ): OpenTransactionId | undefined {
    if (!writeContextJson) {
      return undefined;
    }
    const writeContext = JSON.parse(writeContextJson) as { transaction_id?: unknown };
    return typeof writeContext.transaction_id === "string"
      ? (writeContext.transaction_id as OpenTransactionId)
      : undefined;
  }

  const receipt = (writeContextJson: string | null | undefined, id: string): WriteReceipt => {
    const openTransactionId = openTransactionIdFromWriteContext(writeContextJson);
    return openTransactionId
      ? { kind: "staged", openTransactionId }
      : { kind: "committed", txId: id as TxId };
  };

  const runtime = {
    updateAuth: vi.fn<(auth_json: string) => void>(),
    onAuthFailure: vi.fn<(callback: (reason: string) => void) => void>(),
    // Runtime interface stubs
    insert: vi.fn(
      (table: string, values: any, writeContextJson?: string | null, objectId?: string | null) => {
        return {
          id: objectId ?? "todo-transaction-query",
          values: [],
          ...receipt(writeContextJson, "transaction-query"),
        };
      },
    ),
    restore: vi.fn(
      (table: string, objectId: string, values: any, writeContextJson?: string | null) => {
        return {
          id: objectId,
          values: [],
          ...receipt(writeContextJson, "transaction-query"),
        };
      },
    ),
    update: vi.fn(
      (_table: string, _objectId: string, _values: any, writeContextJson?: string | null) =>
        receipt(writeContextJson, "transaction-update"),
    ),
    upsert: vi.fn(
      (table: string, objectId: string, values: any, writeContextJson?: string | null) =>
        receipt(writeContextJson, "transaction-upsert"),
    ),
    delete: vi.fn((_table: string, _objectId: string, writeContextJson?: string | null) =>
      receipt(writeContextJson, "transaction-delete"),
    ),
    query:
      vi.fn<
        (
          query_json: string,
          session_json?: string | null,
          tier?: string | null,
          options_json?: string | null,
        ) => Promise<any>
      >(),
    createSubscription:
      vi.fn<
        (
          query_json: string,
          session_json?: string | null,
          tier?: string | null,
          options_json?: string | null,
        ) => number
      >(),
    executeSubscription: vi.fn<(handle: number, on_update: Function) => void>(),
    unsubscribe: vi.fn<(handle: number) => void>(),
    onMutationError: vi.fn<Runtime["onMutationError"]>((callback) => {
      mutationErrorCallback = callback;
    }),
    beginTransaction: vi.fn<TransactionalRuntime["beginTransaction"]>((_kind, id) => {
      nextTransactionNumber += 1;
      return id;
    }),
    connect: vi.fn<Runtime["connect"]>(),
    disconnect: vi.fn<Runtime["disconnect"]>(),
    commitTransaction: vi.fn<TransactionalRuntime["commitTransaction"]>(
      () => `committed-${nextTransactionNumber}` as TxId,
    ),
    waitForTransaction: vi.fn<Runtime["waitForTransaction"]>(async () => undefined),
    rollbackTransaction: vi.fn<TransactionalRuntime["rollbackTransaction"]>(async () => false),
    close: vi.fn(),
  } satisfies TransactionalRuntime;

  return Object.assign(runtime, {
    emitMutationError(event: MutationErrorEvent) {
      mutationErrorCallback?.(event);
    },
  });
}

function makeContext(): AppContext {
  return {
    appId: "test-app",
    schema: {},
    serverUrl: "https://example.test",
    jwtToken: "initial.jwt.token",
  };
}

describe("JazzClient onAuthFailure wiring", () => {
  it("registers runtimeOptions.onAuthFailure with runtime.onAuthFailure on construction", () => {
    const runtime = makeFakeRuntime();
    const onAuthFailure = vi.fn();

    JazzClient.connectWithRuntime(runtime as any, makeContext(), { onAuthFailure });

    expect(runtime.onAuthFailure).toHaveBeenCalledTimes(1);

    // Invoke whatever callback was registered:
    const registered = runtime.onAuthFailure.mock.calls[0][0];
    registered("token expired");
    expect(onAuthFailure).toHaveBeenCalledWith("expired");
  });

  it("does nothing when runtimeOptions.onAuthFailure is omitted", () => {
    const runtime = makeFakeRuntime();
    JazzClient.connectWithRuntime(runtime as any, makeContext(), {});
    expect(runtime.onAuthFailure).not.toHaveBeenCalled();
  });
});

describe("JazzClient subscription ownership", () => {
  it("releases a created handle when synchronous callback installation throws", () => {
    const runtime = makeFakeRuntime();
    const failure = new Error("executeSubscription failed after callback");
    runtime.createSubscription.mockReturnValue(41);
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      onUpdate({ added: [], updated: [], removed: [] });
      throw failure;
    });
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const callback = vi.fn();

    expect(() => client.subscribe('{"table":"todos"}', callback)).toThrow(failure);

    expect(callback).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledWith(41);
  });
  it("delivers established runtime failures through the subscription error callback", () => {
    const runtime = makeFakeRuntime();
    const failure = new Error("subscription stream failed");
    runtime.createSubscription.mockReturnValue(42);
    let emit!: (result: RuntimeSubscriptionDelta | Error) => void;
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      emit = onUpdate as typeof emit;
    });
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());
    const callback = vi.fn();
    const onError = vi.fn();

    expect(() =>
      client.subscribe('{"table":"todos"}', { onUpdate: callback, onError }),
    ).not.toThrow();
    emit(failure);
    emit(new Error("late duplicate failure"));

    expect(callback).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(failure);
    expect(runtime.unsubscribe).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledWith(42);
  });

  it("contains terminal error callback failures after releasing the native handle", () => {
    const runtime = makeFakeRuntime();
    const failure = new Error("subscription stream failed");
    const callbackFailure = new Error("terminal callback failed");
    let emit!: (result: RuntimeSubscriptionDelta | Error) => void;
    runtime.createSubscription.mockReturnValue(45);
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      emit = onUpdate as typeof emit;
    });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());

    client.subscribe('{"table":"todos"}', {
      onUpdate: vi.fn(),
      onError: () => {
        throw callbackFailure;
      },
    });
    expect(() => emit(failure)).not.toThrow();

    expect(runtime.unsubscribe).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledWith(45);
    expect(consoleError).toHaveBeenCalledWith(
      "Jazz subscription error callback failed",
      callbackFailure,
    );
    consoleError.mockRestore();
  });

  it("releases a terminal subscription exactly once when onError reentrantly unsubscribes", () => {
    const runtime = makeFakeRuntime();
    const failure = new Error("subscription stream failed");
    let emit!: (result: RuntimeSubscriptionDelta | Error) => void;
    runtime.createSubscription.mockReturnValue(46);
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      emit = onUpdate as typeof emit;
    });
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());
    const id = client.subscribe('{"table":"todos"}', {
      onUpdate: vi.fn(),
      onError: () => client.unsubscribe(id),
    });

    emit(failure);
    emit({ added: [], updated: [], removed: [] });

    expect(runtime.unsubscribe).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledWith(46);
  });

  it("fences a queued terminal frame after public unsubscribe without releasing twice", () => {
    const runtime = makeFakeRuntime();
    let emit!: (result: RuntimeSubscriptionDelta | Error) => void;
    runtime.createSubscription.mockReturnValue(47);
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      emit = onUpdate as typeof emit;
    });
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());
    const onError = vi.fn();
    const id = client.subscribe('{"table":"todos"}', { onUpdate: vi.fn(), onError });

    client.unsubscribe(id);
    emit(new Error("queued terminal failure"));
    client.unsubscribe(id);

    expect(onError).not.toHaveBeenCalled();
    expect(runtime.unsubscribe).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledWith(47);
  });

  it("terminalizes only the affected subscription and keeps siblings live", () => {
    const runtime = makeFakeRuntime();
    const callbacks = new Map<number, (result: RuntimeSubscriptionDelta | Error) => void>();
    runtime.createSubscription.mockReturnValueOnce(51).mockReturnValueOnce(52);
    runtime.executeSubscription.mockImplementation((handle, onUpdate) => {
      callbacks.set(handle, onUpdate as (result: RuntimeSubscriptionDelta | Error) => void);
    });
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());
    const affectedUpdate = vi.fn();
    const affectedError = vi.fn();
    const siblingUpdate = vi.fn();
    const siblingError = vi.fn();

    client.subscribe('{"table":"affected"}', { onUpdate: affectedUpdate, onError: affectedError });
    client.subscribe('{"table":"sibling"}', { onUpdate: siblingUpdate, onError: siblingError });

    const failure = new Error("affected subscription failed");
    callbacks.get(51)!(failure);
    callbacks.get(51)!({ added: [], updated: [], removed: [] });
    callbacks.get(52)!({ added: [], updated: [], removed: [] });

    expect(affectedError).toHaveBeenCalledTimes(1);
    expect(affectedError).toHaveBeenCalledWith(failure);
    expect(affectedUpdate).not.toHaveBeenCalled();
    expect(siblingError).not.toHaveBeenCalled();
    expect(siblingUpdate).toHaveBeenCalledTimes(1);
  });

  it("reports a legacy function-form terminal error and fences later updates", () => {
    const runtime = makeFakeRuntime();
    const failure = new Error("legacy subscription failed");
    let emit!: (result: RuntimeSubscriptionDelta | Error) => void;
    runtime.createSubscription.mockReturnValue(43);
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      emit = onUpdate as typeof emit;
    });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());
    const onUpdate = vi.fn();

    client.subscribe('{"table":"todos"}', onUpdate);
    emit(failure);
    emit({ added: [], updated: [], removed: [] });
    emit(new Error("duplicate failure"));

    expect(consoleError).toHaveBeenCalledOnce();
    expect(consoleError).toHaveBeenCalledWith("Unhandled Jazz subscription error", failure);
    expect(onUpdate).not.toHaveBeenCalled();
  });

  it("routes update callback failures through onError and contains onError exceptions", () => {
    const runtime = makeFakeRuntime();
    const callbackFailure = new Error("subscription callback failed");
    const errorCallbackFailure = new Error("subscription error callback failed");
    let emit!: (result: RuntimeSubscriptionDelta | Error) => void;
    runtime.createSubscription.mockReturnValue(44);
    runtime.executeSubscription.mockImplementation((_handle, onUpdate) => {
      emit = onUpdate as typeof emit;
    });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const client = JazzClient.connectWithRuntime(runtime as unknown as Runtime, makeContext());
    const onUpdate = vi.fn(() => {
      throw callbackFailure;
    });
    const onError = vi.fn(() => {
      throw errorCallbackFailure;
    });

    client.subscribe('{"table":"todos"}', { onUpdate, onError });
    expect(() => emit({ added: [], updated: [], removed: [] })).not.toThrow();
    emit({ added: [], updated: [], removed: [] });

    expect(onUpdate).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(callbackFailure);
    expect(runtime.unsubscribe).toHaveBeenCalledOnce();
    expect(runtime.unsubscribe).toHaveBeenCalledWith(44);
    expect(consoleError).toHaveBeenCalledWith(
      "Jazz subscription error callback failed",
      errorCallbackFailure,
    );
  });
});

describe("JazzClient native session boundary", () => {
  it("keeps public author out of serialized query sessions", async () => {
    const runtime = makeFakeRuntime();
    runtime.query.mockResolvedValue([]);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const session = {
      issuer: "https://issuer.example",
      user_id: "alice",
      claims: { role: "reader" },
      authMode: "external",
    } as const;

    await client.query('{"table":"todos"}', undefined, session);

    const serialized = JSON.parse(runtime.query.mock.calls[0][1] ?? "null");
    expect(serialized).toEqual({
      issuer: "https://issuer.example",
      user_id: "alice",
      claims: { role: "reader" },
      authMode: "external",
    });
    expect(serialized).not.toHaveProperty("author");
  });
});

describe("JazzClient.updateAuthToken", () => {
  it("forwards refreshed JWT to the Rust runtime via runtime.updateAuth", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    client.updateAuthToken("new.jwt.token");

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: "new.jwt.token" });
  });

  it("forwards undefined JWT (clear) as null jwt_token", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    client.updateAuthToken(undefined);

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: null });
  });

  it("preserves admin_secret from context across token refresh", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      adminSecret: "admin-xyz",
    });

    client.updateAuthToken("new.jwt.token");

    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({
      jwt_token: "new.jwt.token",
      admin_secret: "admin-xyz",
    });
  });

  it("preserves backend_secret from context across token refresh", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      backendSecret: "backend-abc",
    });

    client.updateAuthToken("new.jwt.token");

    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({
      jwt_token: "new.jwt.token",
      backend_secret: "backend-abc",
    });
  });
});

describe("JazzClient.updateCookieSession", () => {
  it("refreshes transport auth without inventing backend session auth", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "cookie-app",
      schema: {},
      serverUrl: "https://example.test",
      cookieSession: {
        user_id: "alice",
        claims: {
          role: "reader",
          auth_mode: "external",
          subject: "alice-subject",
          issuer: "https://issuer.example",
        },
        issuer: "https://issuer.example",
        authMode: "external",
      },
    });

    client.updateCookieSession({
      user_id: "alice",
      claims: {
        role: "writer",
        auth_mode: "external",
        subject: "alice-subject",
        issuer: "https://issuer.example",
      },
      issuer: "https://issuer.example",
      authMode: "external",
    });

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: null });
    expect(JSON.parse(arg)).not.toHaveProperty("backend_session");
  });

  it("forwards cookie session as backend_session when backend auth is configured", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      backendSecret: "backend-secret",
      cookieSession: {
        user_id: "00000000-0000-0000-0000-000000000001",
        claims: { role: "reader" },
        issuer: "https://issuer.example",
        authMode: "external",
      },
    });

    const refreshed = {
      issuer: "https://issuer.example",
      user_id: "00000000-0000-0000-0000-000000000001",
      claims: { role: "writer" },
      authMode: "external" as const,
    };
    client.updateCookieSession(refreshed);

    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({
      jwt_token: "initial.jwt.token",
      backend_secret: "backend-secret",
      backend_session: refreshed,
    });
  });
});

describe("resolveDefaultDurabilityTier", () => {
  it("uses local as the default offline durability tier", () => {
    expect(resolveDefaultDurabilityTier({})).toBe("local");
  });

  it("still prefers edge when a server is configured outside the browser runtime", () => {
    expect(resolveDefaultDurabilityTier({ serverUrl: "https://example.test" })).toBe("edge");
  });
});

describe("public read tiers", () => {
  it("lowers each new public tier to the existing native durability contract", () => {
    expect(resolveReadTier("local-first")).toBe("local");
    expect(resolveReadTier("remote")).toBe("edge");
    expect(resolveReadTier("remote-if-possible")).toBe("edge");
    expect(resolveReadTier(ReadTier.LocalFirst)).toBe("local");
    expect(resolveReadTier(ReadTier.Remote)).toBe("edge");
    expect(resolveReadTier(ReadTier.RemoteIfPossible)).toBe("edge");
  });

  it("keeps legacy read durability controls byte-for-byte compatible", () => {
    for (const tier of ["local", "edge", "global"] as const) {
      expect(resolveReadTier(tier)).toBe(tier);
      expect(resolveEffectiveQueryExecutionOptions({}, { tier })).toMatchObject({ tier });
    }
  });

  it("does not reinterpret remote-if-possible as a third native tier", () => {
    expect(
      resolveEffectiveQueryExecutionOptions({}, { tier: ReadTier.RemoteIfPossible }),
    ).toMatchObject({
      tier: "edge",
    });
  });
});

describe("JazzClient schema access", () => {
  it("returns the schema from the client context", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    };
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "schema-context-app",
      schema,
    });

    expect(client.getSchema()).toBe(schema);
    expect(client.getSchema()).toBe(schema);
  });
});

describe("JazzClient transaction query plumbing", () => {
  it("encodes exact and head-over-base branch mutation targets", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const head = { values: { workspace: { type: "Integer", value: 7 } as const } };
    const base = { values: { workspace: { type: "Integer", value: 1 } as const } };

    client.insert("todos", {}, { branch: head });
    client.update(
      "todos",
      "00000000-0000-0000-0000-000000000001",
      {},
      {
        branch: { head, base: { kind: "current", branch: base } },
      },
    );
    client.upsert(
      "todos",
      "00000000-0000-0000-0000-000000000002",
      {},
      {
        branch: { head, base: { kind: "current", branch: base } },
      },
    );

    expect(JSON.parse(runtime.insert.mock.calls[0][2] as string)).toMatchObject({
      branch_view: { head: { values: { workspace: [1, 4, 7, 0, 0, 0] } } },
    });
    expect(JSON.parse(runtime.update.mock.calls[0][3] as string)).toMatchObject({
      branch_view: {
        head: { values: { workspace: [1, 4, 7, 0, 0, 0] } },
        base: { Current: { values: { workspace: [1, 4, 1, 0, 0, 0] } } },
      },
    });
    expect(JSON.parse(runtime.upsert.mock.calls[0][3] as string)).toMatchObject({
      branch_view: {
        head: { values: { workspace: [1, 4, 7, 0, 0, 0] } },
        base: { Current: { values: { workspace: [1, 4, 1, 0, 0, 0] } } },
      },
    });
  });

  it("encodes ergonomic branch selectors into the native read view", async () => {
    const runtime = makeFakeRuntime();
    runtime.query.mockResolvedValue([]);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    await client.query(JSON.stringify({ relation_ir: { table: "todos" } }), {
      branch: {
        head: { values: { workspace: { type: "Integer", value: 7 } } },
        base: {
          kind: "current",
          branch: {
            values: {
              workspace: { type: "Integer", value: 1 },
              tenant: { type: "Uuid", value: "42424242-4242-4242-4242-424242424242" },
            },
          },
        },
      },
    });

    const optionsJson = runtime.query.mock.calls[0][3];
    expect(JSON.parse(optionsJson as string)).toMatchObject({
      read_view: {
        source: {
          BranchView: {
            head: { values: { workspace: [1, 4, 7, 0, 0, 0] } },
            base: {
              Current: {
                values: {
                  workspace: [1, 4, 1, 0, 0, 0],
                  tenant: [1, 7, ...Array(16).fill(0x42)],
                },
              },
            },
          },
        },
      },
    });
  });

  it("canonically encodes string branch values", async () => {
    const runtime = makeFakeRuntime();
    runtime.query.mockResolvedValue([]);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    await client.query(JSON.stringify({ relation_ir: { table: "todos" } }), {
      branch: {
        head: { values: { branch: { type: "Text", value: "draft" } } },
      },
    });

    const optionsJson = runtime.query.mock.calls[0][3];
    expect(JSON.parse(optionsJson as string)).toMatchObject({
      read_view: {
        source: {
          BranchView: {
            head: { values: { branch: [1, 6, 2, 100, 114, 97, 102, 116] } },
          },
        },
      },
    });
  });

  it.each([
    { type: "Integer", value: 0x80000000 },
    { type: "Integer", value: 1.5 },
    { type: "BigInt", value: Number.MAX_SAFE_INTEGER + 1 },
    { type: "BigInt", value: 1n << 63n },
  ] as const)("rejects invalid branch column value $type:$value", async (value) => {
    const runtime = makeFakeRuntime();
    runtime.query.mockResolvedValue([]);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    await expect(
      client.query(JSON.stringify({ relation_ir: { table: "todos" } }), {
        branch: { head: { values: { workspace: value } } },
      }),
    ).rejects.toThrow(/branch (Integer|BigInt) values/);
    expect(runtime.query).not.toHaveBeenCalled();
  });

  it("supports raw reads scoped to the open transaction", async () => {
    const runtime = makeFakeRuntime();
    runtime.query.mockResolvedValue([{ id: "todo-transaction-query", values: [] }]);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const transactionId = client.beginTransaction("mergeable");

    client.insertInternal("todos", {}, undefined, undefined, undefined, transactionId);

    await expect(
      client.query(JSON.stringify({ relation_ir: { table: "todos" } }), {
        localUpdates: "deferred",
        openTransactionId: transactionId,
      }),
    ).resolves.toEqual([{ id: "todo-transaction-query", values: [] }]);

    expect(runtime.query).toHaveBeenCalledTimes(1);
    const optionsJson = runtime.query.mock.calls[0][3];
    expect(JSON.parse(optionsJson as string)).toMatchObject({
      local_updates: "deferred",
      transaction_id: transactionId,
    });
  });
});

describe("JazzClient runtime transaction waits", () => {
  it("delegates unsettled waits to the runtime", async () => {
    const runtime = makeFakeRuntime();
    runtime.waitForTransaction = vi.fn(async () => undefined);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    await expect(
      client.waitForTransaction("transaction-runtime" as TxId, "edge"),
    ).resolves.toBeUndefined();

    expect(runtime.waitForTransaction).toHaveBeenCalledWith("transaction-runtime", "edge");
  });

  it("waits for connected exclusive transactions at the global tier", async () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const handle = new ExclusiveWriteHandle("transaction-exclusive" as TxId, client);

    await expect(handle.wait()).resolves.toBeUndefined();

    expect(runtime.waitForTransaction).toHaveBeenCalledWith("transaction-exclusive", "global");
  });

  it("waits for local-only exclusive transactions at the local tier", async () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      serverUrl: undefined,
    });
    const handle = new ExclusiveWriteHandle("transaction-exclusive" as TxId, client);

    await expect(handle.wait()).resolves.toBeUndefined();

    expect(runtime.waitForTransaction).toHaveBeenCalledWith("transaction-exclusive", "local");
  });

  it("surfaces runtime wait rejection as PersistedWriteRejectedError", async () => {
    const runtime = makeFakeRuntime();
    const txId = "transaction-runtime-rejected" as TxId;
    let rejectWait!: (error: unknown) => void;
    runtime.waitForTransaction = vi.fn(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectWait = reject;
        }),
    );
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    const waitPromise = client.waitForTransaction(txId, "edge");
    await Promise.resolve();

    rejectWait({
      kind: "rejected",
      transactionId: txId,
      code: "permission_denied",
      reason: "write rejected by policy",
    });

    await expect(waitPromise).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      transactionId: txId,
      message: `Persisted transaction ${txId} was rejected (permission_denied): write rejected by policy`,
    });
  });
});

describe("JazzClient mutation error handling", () => {
  function makeRejectedTransactionRecord(transactionId: TxId) {
    return {
      transactionId,
      kind: "mergeable" as const,
      sealed: true,
      latestSettlement: {
        kind: "rejected" as const,
        transactionId,
        code: "permission_denied",
        reason: "write rejected by policy",
      },
    };
  }

  it("forwards pushed runtime mutation errors to the registered listener", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const listener = vi.fn();
    client.onMutationError(listener);
    const txId = "batch-rejected" as TxId;
    const event: MutationErrorEvent = {
      code: "permission_denied",
      reason: "write rejected by policy",
      transaction: makeRejectedTransactionRecord(txId),
    };

    runtime.emitMutationError(event);

    expect(listener).toHaveBeenCalledWith(event);
  });

  it("logs pushed mutation errors when no application listener replaces the fallback", () => {
    const runtime = makeFakeRuntime();
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    JazzClient.connectWithRuntime(runtime as any, makeContext());
    const txId = "batch-unhandled" as TxId;
    const event: MutationErrorEvent = {
      code: "permission_denied",
      reason: "write rejected by policy",
      transaction: makeRejectedTransactionRecord(txId),
    };

    runtime.emitMutationError(event);

    expect(consoleError).toHaveBeenCalledWith("Unhandled Jazz mutation error", event);
  });
});

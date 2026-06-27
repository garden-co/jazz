import { describe, it, expect, vi } from "vitest";
import {
  ExclusiveWriteHandle,
  JazzClient,
  resolveDefaultDurabilityTier,
  type MutationErrorEvent,
  type Runtime,
  type TransactionalRuntime,
  PersistedWriteRejectedError,
} from "./client.js";
import type { AppContext } from "./context.js";
import type { WasmSchema } from "../drivers/types.js";

function makeFakeRuntime() {
  let mutationErrorCallback: ((event: MutationErrorEvent) => void) | null = null;
  let nextTransactionNumber = 0;

  function transactionIdFromWriteContext(writeContextJson?: string | null): string | undefined {
    if (!writeContextJson) {
      return undefined;
    }
    const writeContext = JSON.parse(writeContextJson) as { batch_id?: unknown };
    return typeof writeContext.batch_id === "string" ? writeContext.batch_id : undefined;
  }

  const runtime = {
    updateAuth: vi.fn<(auth_json: string) => void>(),
    onAuthFailure: vi.fn<(callback: (reason: string) => void) => void>(),
    // Runtime interface stubs
    insert: vi.fn(
      (table: string, values: any, writeContextJson?: string | null, objectId?: string | null) => {
        const transactionId = transactionIdFromWriteContext(writeContextJson);
        return {
          id: objectId ?? "todo-transaction-query",
          values: [],
          transactionId: transactionId ?? "transaction-query",
        };
      },
    ),
    restore: vi.fn(
      (table: string, objectId: string, values: any, writeContextJson?: string | null) => {
        const transactionId = transactionIdFromWriteContext(writeContextJson);
        return {
          id: objectId,
          values: [],
          transactionId: transactionId ?? "transaction-query",
        };
      },
    ),
    update: vi.fn(
      (_table: string, _objectId: string, _values: any, writeContextJson?: string | null) => ({
        transactionId: transactionIdFromWriteContext(writeContextJson) ?? "transaction-update",
      }),
    ),
    upsert: vi.fn(
      (table: string, objectId: string, values: any, writeContextJson?: string | null) => ({
        transactionId: transactionIdFromWriteContext(writeContextJson) ?? "transaction-upsert",
      }),
    ),
    delete: vi.fn((_table: string, _objectId: string, writeContextJson?: string | null) => ({
      transactionId: transactionIdFromWriteContext(writeContextJson) ?? "transaction-delete",
    })),
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
    beginTransaction: vi.fn<TransactionalRuntime["beginTransaction"]>((kind) => {
      nextTransactionNumber += 1;
      return `transaction-${kind}-${nextTransactionNumber}`;
    }),
    connect: vi.fn<Runtime["connect"]>(),
    disconnect: vi.fn<Runtime["disconnect"]>(),
    commitTransaction: vi.fn<(transaction_id: string) => void>(),
    waitForTransaction: vi.fn<Runtime["waitForTransaction"]>(async () => undefined),
    rollbackTransaction: vi.fn<TransactionalRuntime["rollbackTransaction"]>(() => false),
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
        authMode: "external",
      },
    });

    const refreshed = {
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
  it("supports raw reads scoped to the open transaction", async () => {
    const runtime = makeFakeRuntime();
    runtime.query.mockResolvedValue([{ id: "todo-transaction-query", values: [] }]);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const transactionId = client.beginTransaction("mergeable");

    client.insertInternal("todos", {}, undefined, undefined, undefined, transactionId);

    await expect(
      client.query(
        { _build: () => JSON.stringify({ table: "todos" }) },
        {
          localUpdates: "deferred",
          transactionId,
        },
      ),
    ).resolves.toEqual([{ id: "todo-transaction-query", values: [] }]);

    expect(runtime.query).toHaveBeenCalledTimes(1);
    const optionsJson = runtime.query.mock.calls[0][3];
    expect(JSON.parse(optionsJson as string)).toMatchObject({
      local_updates: "deferred",
      transaction_batch_id: transactionId,
    });
  });
});

describe("JazzClient runtime transaction waits", () => {
  it("delegates unsettled waits to the runtime", async () => {
    const runtime = makeFakeRuntime();
    runtime.waitForTransaction = vi.fn(async () => undefined);
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    await expect(client.waitForTransaction("transaction-runtime", "edge")).resolves.toBeUndefined();

    expect(runtime.waitForTransaction).toHaveBeenCalledWith("transaction-runtime", "edge");
  });

  it("waits for connected exclusive transactions at the global tier", async () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const handle = new ExclusiveWriteHandle("transaction-exclusive", client);

    await expect(handle.wait()).resolves.toBeUndefined();

    expect(runtime.waitForTransaction).toHaveBeenCalledWith("transaction-exclusive", "global");
  });

  it("waits for local-only exclusive transactions at the local tier", async () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      serverUrl: undefined,
    });
    const handle = new ExclusiveWriteHandle("transaction-exclusive", client);

    await expect(handle.wait()).resolves.toBeUndefined();

    expect(runtime.waitForTransaction).toHaveBeenCalledWith("transaction-exclusive", "local");
  });

  it("lets a runtime wait handle rejection without replaying onMutationError", async () => {
    const runtime = makeFakeRuntime();
    const transactionId = "transaction-runtime-rejected";
    let rejectWait!: (error: unknown) => void;
    runtime.waitForTransaction = vi.fn(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectWait = reject;
        }),
    );
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const seen: MutationErrorEvent[] = [];
    client.onMutationError((event) => {
      seen.push(event);
    });

    const waitPromise = client.waitForTransaction(transactionId, "edge");
    await Promise.resolve();

    rejectWait({
      kind: "rejected",
      transactionId: transactionId,
      code: "permission_denied",
      reason: "write rejected by policy",
    });

    await expect(waitPromise).rejects.toBeInstanceOf(PersistedWriteRejectedError);
    expect(seen).toEqual([]);
  });
});

describe("JazzClient mutation error handling", () => {
  function makeRejectedTransactionRecord(transactionId: string) {
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

  it("receives pushed runtime mutation errors without scanning all transaction records", async () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "queued-rejection-app",
      schema: {},
    });

    const seen: MutationErrorEvent[] = [];

    client.onMutationError((event) => {
      seen.push(event);
    });

    runtime.emitMutationError({
      code: "permission_denied",
      reason: "write rejected by policy",
      transaction: makeRejectedTransactionRecord("transaction-rejected"),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(seen).toEqual([
      {
        code: "permission_denied",
        reason: "write rejected by policy",
        transaction: makeRejectedTransactionRecord("transaction-rejected"),
      },
    ]);
  });

  it("logs pushed runtime mutation errors when no listener is registered", async () => {
    const runtime = makeFakeRuntime();
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    JazzClient.connectWithRuntime(runtime as any, {
      appId: "sync-rejection-app",
      schema: {},
    });

    const event: MutationErrorEvent = {
      code: "permission_denied",
      reason: "write rejected by policy",
      transaction: makeRejectedTransactionRecord("transaction-rejected"),
    };
    runtime.emitMutationError(event);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(consoleError).toHaveBeenCalledWith("Unhandled Jazz mutation error", event);

    consoleError.mockRestore();
  });

  it("flushes pending runtime mutation errors during callback registration", async () => {
    const runtime = makeFakeRuntime();
    runtime.onMutationError = vi.fn((callback) => {
      callback({
        code: "permission_denied",
        reason: "write rejected by policy",
        transaction: makeRejectedTransactionRecord("transaction-rejected"),
      });
    });

    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    JazzClient.connectWithRuntime(runtime as any, {
      appId: "startup-rejection-app",
      schema: {},
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(consoleError).toHaveBeenCalledTimes(1);
    consoleError.mockRestore();
  });
});

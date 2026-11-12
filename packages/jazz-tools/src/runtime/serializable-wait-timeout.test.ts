import { afterEach, expect, it, vi } from "vitest";
import {
  JazzClient,
  PersistedWriteRejectedError,
  SerializableWriteHandle,
  SerializableWriteResult,
  WriteWaitTimeoutError,
  type TxId,
} from "./client.js";
import {
  runtimeTransactionRecordStubs,
  testTxId,
  type Runtime,
  type AppContext,
} from "./client-tests/support.js";

function fixture(upstream: "none" | "server" | "native" = "none") {
  const calls: Array<{ tier: string; signal: AbortSignal }> = [];
  const accepted: Array<() => void> = [];
  const runtime = {
    ...runtimeTransactionRecordStubs,
    nativeUpstreamConfigured: () => upstream === "native",
    // Like the native runtime: await the ID, then settle by tier.
    waitForTransaction: (id: unknown, tier: string, options: { signal: AbortSignal }) =>
      new Promise<void>((resolve, reject) => {
        const signal = options.signal;
        calls.push({ tier, signal });
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        Promise.resolve(id).then(() => {
          if (!signal.aborted) {
            if (tier === "local") resolve();
            else accepted.push(resolve);
          }
        }, reject);
      }),
  } as unknown as Runtime;
  const context = {
    appId: "wait-timeout",
    schema: {},
    ...(upstream === "server" ? { serverUrl: "https://authority.example" } : {}),
  } as AppContext;
  const Constructor = JazzClient as unknown as {
    new (runtime: Runtime, context: AppContext, options: object): JazzClient;
  };
  return {
    client: new Constructor(runtime, context, {}),
    calls,
    awaitingAcceptance: () => accepted.length,
    accept: () => accepted.splice(0).forEach((resolve) => resolve()),
  };
}

const kinds = ["handle", "result", "mapped"] as const;

function handle(
  kind: (typeof kinds)[number],
  client: JazzClient,
  id: Promise<TxId> = Promise.resolve(testTxId("committed")),
) {
  if (kind === "handle") return new SerializableWriteHandle(id, client);
  const result = new SerializableWriteResult("value", id, client);
  return kind === "result" ? result : result.mapValue((value) => value.length);
}

function expected(kind: (typeof kinds)[number]) {
  return kind === "handle" ? undefined : kind === "result" ? "value" : 5;
}

afterEach(() => vi.useRealTimers());

it.each(["none", "server", "native"] as const)(
  "default wait keeps the %s upstream tier",
  async (upstream) => {
    const f = fixture(upstream);
    const wait = handle("result", f.client).wait({ timeoutMs: 1000 });
    await vi.waitFor(() => expect(f.calls).toHaveLength(1));
    expect(f.calls[0]?.tier).toBe(upstream === "none" ? "local" : "global");
    if (upstream !== "none") await vi.waitFor(() => expect(f.awaitingAcceptance()).toBe(1));
    f.accept();
    await expect(wait).resolves.toBe("value");
  },
);

it.each(kinds)(
  "%s times out before a stalled commit returns its ID and can wait again afterwards",
  async (kind) => {
    vi.useFakeTimers();
    const f = fixture();
    let resolveId!: (id: TxId) => void;
    const id = new Promise<TxId>((resolve) => {
      resolveId = resolve;
    });
    const h = handle(kind, f.client, id);
    let state = "pending";
    const wait = h.wait({ timeoutMs: 25 }).then(
      (value) => {
        state = "settled";
        return value;
      },
      (error) => {
        state = "ended";
        return error;
      },
    );
    await vi.advanceTimersByTimeAsync(24);
    expect(state).toBe("pending");
    await vi.advanceTimersByTimeAsync(1);
    expect(state).toBe("ended");
    expect(await wait).toBeInstanceOf(WriteWaitTimeoutError);
    expect(await wait).toMatchObject({ tier: "local", timeoutMs: 25 });
    expect(f.calls.every((call) => call.signal.aborted)).toBe(true);
    resolveId(testTxId("later"));
    await expect(h.wait()).resolves.toBe(expected(kind));
  },
);

it.each([
  ["an Error", new Error("commit failed")],
  ["a non-Error value", "commit failed"],
  [
    "a rejection-shaped value",
    { kind: "rejected", transactionId: "tx", code: "denied", reason: "commit failed" },
  ],
] as const)("surfaces a failed commit's own error unchanged (%s)", async (_label, failure) => {
  for (const timeoutMs of [undefined, 1000]) {
    const f = fixture("server");
    const id = Promise.reject(failure);
    id.catch(() => undefined);
    const caught = await new SerializableWriteHandle(id, f.client)
      .wait(timeoutMs === undefined ? {} : { timeoutMs })
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    expect(caught).toBe(failure);
    expect(caught).not.toBeInstanceOf(PersistedWriteRejectedError);
  }
});

it.each(kinds)("%s rejects an invalid timeout before registering a runtime wait", async (kind) => {
  const f = fixture();
  const h = handle(kind, f.client);
  await expect(h.wait({ timeoutMs: -1 })).rejects.toThrow(/do not retry/);
  expect(f.calls).toHaveLength(0);
});

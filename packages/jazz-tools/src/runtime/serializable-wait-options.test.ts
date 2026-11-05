import { afterEach, expect, it, vi } from "vitest";
import {
  JazzClient,
  SerializableWriteHandle,
  SerializableWriteResult,
  WriteWaitTimeoutError,
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
    appId: "wait-options",
    schema: {},
    ...(upstream === "server" ? { serverUrl: "https://authority.example" } : {}),
  } as AppContext;
  const Constructor = JazzClient as unknown as {
    new (runtime: Runtime, context: AppContext, options: object): JazzClient;
  };
  return {
    client: new Constructor(runtime, context, {}),
    calls,
    accept: () => accepted.splice(0).forEach((resolve) => resolve()),
  };
}
const kinds = ["handle", "result", "mapped"] as const;
function handle(
  kind: (typeof kinds)[number],
  client: JazzClient,
  id = Promise.resolve(testTxId("committed")),
) {
  if (kind === "handle") return new SerializableWriteHandle(id, client);
  const result = new SerializableWriteResult("value", id, client);
  return kind === "result" ? result : result.mapValue((value) => value.length);
}
function expected(kind: (typeof kinds)[number]) {
  return kind === "handle" ? undefined : kind === "result" ? "value" : 5;
}
afterEach(() => vi.useRealTimers());
it.each(kinds)("%s keeps Global pending after offline Local succeeds", async (kind) => {
  const f = fixture();
  const h = handle(kind, f.client);
  let done = false;
  const global = h.wait({ tier: "global", timeoutMs: 1000 }).then((value) => {
    done = true;
    return value;
  });
  await expect(h.wait()).resolves.toBe(expected(kind));
  expect(done).toBe(false);
  expect(f.calls.map((call) => call.tier)).toEqual(["global", "local"]);
  f.accept();
  await expect(global).resolves.toBe(expected(kind));
  expect(f.calls.every((call) => call.signal.aborted)).toBe(true);
});
it.each(["none", "server", "native"] as const)(
  "default wait preserves the %s upstream tier",
  async (upstream) => {
    const f = fixture(upstream);
    const wait = handle("result", f.client).wait({ timeoutMs: 1000 });
    await vi.waitFor(() => expect(f.calls).toHaveLength(1));
    expect(f.calls[0]?.tier).toBe(upstream === "none" ? "local" : "global");
    f.accept();
    await expect(wait).resolves.toBe("value");
  },
);
it.each(kinds)(
  "%s times out before the commit returns its ID and can wait locally afterwards",
  async (kind) => {
    vi.useFakeTimers();
    const f = fixture();
    let resolveId!: (id: ReturnType<typeof testTxId>) => void;
    const id = new Promise<ReturnType<typeof testTxId>>((resolve) => {
      resolveId = resolve;
    });
    const h = handle(kind, f.client, id);
    let state = "pending";
    const wait = h.wait({ tier: "global", timeoutMs: 25 }).then(
      (value) => {
        state = "accepted";
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
    expect(await wait).toMatchObject({ tier: "global", timeoutMs: 25 });
    expect(f.calls.every((call) => call.signal.aborted)).toBe(true);
    resolveId(testTxId("later"));
    await Promise.resolve();
    await expect(h.wait({ tier: "local" })).resolves.toBe(expected(kind));
  },
);
it.each(kinds)("%s rejects invalid options before registering a runtime wait", async (kind) => {
  const f = fixture();
  const h = handle(kind, f.client);
  await expect(h.wait({ timeoutMs: -1 })).rejects.toThrow(/do not retry/);
  await expect(h.wait({ tier: "edge" } as never)).rejects.toThrow(/do not retry/);
  expect(f.calls).toHaveLength(0);
});

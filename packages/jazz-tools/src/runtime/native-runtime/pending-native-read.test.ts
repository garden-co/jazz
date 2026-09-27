import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";

// These boundary tests deliberately control the native future. Public query
// tests cannot distinguish a real wake from a host that silently polls forever.
// Real WASM/worker result correctness is exercised by the browser integration.
const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
type PendingRead = { poll(): Uint8Array | null; cancel(): void; setWake(wake: () => void): void };
type ReadHost = {
  awaitNativeRead(read: PendingRead, tier?: string): Promise<Uint8Array>;
  pumpServerTransport(): Promise<void>;
  resolveServerTransportErrorWaiters(error: Error): void;
};
const runtimes: NativeRuntimeAdapter[] = [];
function runtime() {
  const db = {
    setTickScheduler() {},
    tick() {},
    close() {},
    registerSchema() {
      return db;
    },
  };
  const instance = new NativeRuntimeAdapter(
    null,
    app.wasmSchema,
    new Uint8Array(16),
    testAuthorBytes("pending-read-wake"),
    1,
    true,
    { db: db as never },
  );
  runtimes.push(instance);
  return instance;
}
const host = (runtime: NativeRuntimeAdapter) => runtime as unknown as ReadHost;
function sleepingRead() {
  let ready = false;
  let wake = () => {};
  let firstPoll = () => {};
  const started = new Promise<void>((resolve) => {
    firstPoll = resolve;
  });
  const bytes = Uint8Array.of(1, 2, 3);
  const pending = {
    poll: vi.fn(() => {
      firstPoll();
      return ready ? bytes : null;
    }),
    cancel: vi.fn(),
    setWake: (callback: () => void) => {
      wake = callback;
    },
  };
  return {
    pending,
    started,
    bytes,
    wake: () => wake(),
    finish: () => {
      ready = true;
      wake();
    },
  };
}
afterEach(async () => {
  await Promise.all(runtimes.splice(0).map((runtime) => runtime.close()));
  vi.restoreAllMocks();
});

describe("wake-driven native reads", () => {
  it("does no native polling while asleep and coalesces a burst of wakes", async () => {
    const read = sleepingRead();
    const result = host(runtime()).awaitNativeRead(read.pending);
    await read.started;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(read.pending.poll).toHaveBeenCalledTimes(1);
    read.finish();
    read.wake();
    read.wake();
    expect(await result).toEqual(read.bytes);
    expect(read.pending.poll).toHaveBeenCalledTimes(2);
    expect(read.pending.cancel).toHaveBeenCalledTimes(1);
  });

  it("retains synchronous CPU wakes and yields to a host timer", async () => {
    let timerFired = false;
    let wake = () => {};
    const bytes = Uint8Array.of(4);
    const timer = setTimeout(() => {
      timerFired = true;
    }, 0);
    const pending = {
      setWake: (callback: () => void) => {
        wake = callback;
      },
      poll: vi.fn(() => {
        if (timerFired) return bytes;
        wake();
        return null;
      }),
      cancel: vi.fn(),
    };
    try {
      expect(await host(runtime()).awaitNativeRead(pending)).toEqual(bytes);
      expect(pending.poll.mock.calls.length).toBeGreaterThan(1);
      expect(pending.cancel).toHaveBeenCalledTimes(1);
    } finally {
      clearTimeout(timer);
    }
  });

  it("lets an independent read complete while another awaits external data", async () => {
    const owner = runtime();
    const slow = sleepingRead();
    const fast = sleepingRead();
    const slowResult = host(owner).awaitNativeRead(slow.pending);
    const fastResult = host(owner).awaitNativeRead(fast.pending);
    await Promise.all([slow.started, fast.started]);
    fast.finish();
    expect(await fastResult).toEqual(fast.bytes);
    expect(slow.pending.poll).toHaveBeenCalledTimes(1);
    slow.finish();
    expect(await slowResult).toEqual(slow.bytes);
  });

  for (const closing of ["facade", "owner"] as const) {
    it(`wakes and cancels a sleeping facade read when its ${closing} closes`, async () => {
      const owner = runtime();
      const facade = owner.registerSchemaView(app.wasmSchema);
      runtimes.push(facade);
      const read = sleepingRead();
      const result = host(facade).awaitNativeRead(read.pending);
      const rejected = expect(result).rejects.toThrow("runtime shutdown");
      await read.started;
      await (closing === "facade" ? facade : owner).close();
      await rejected;
      expect(read.pending.cancel).toHaveBeenCalledTimes(1);
      expect(read.pending.poll).toHaveBeenCalledTimes(1);
    });
  }

  it("wakes a sleeping strict read when its transport fails", async () => {
    const owner = runtime();
    const read = sleepingRead();
    const result = host(owner).awaitNativeRead(read.pending, "global");
    const rejected = expect(result).rejects.toThrow("authority disconnected");
    await read.started;
    host(owner).resolveServerTransportErrorWaiters(new Error("authority disconnected"));
    await rejected;
    expect(read.pending.poll).toHaveBeenCalledTimes(1);
    expect(read.pending.cancel).toHaveBeenCalledTimes(1);
  });

  it("surfaces a pump failure without waiting for a native wake", async () => {
    const owner = runtime();
    vi.spyOn(host(owner), "pumpServerTransport").mockRejectedValue(new Error("pump failed"));
    const read = sleepingRead();
    await expect(host(owner).awaitNativeRead(read.pending)).rejects.toThrow("pump failed");
    expect(read.pending.poll).toHaveBeenCalledTimes(1);
    expect(read.pending.cancel).toHaveBeenCalledTimes(1);
  });
});

import { afterEach, describe, expect, it, vi } from "vitest";

const { loadWasmModuleMock } = vi.hoisted(() => ({
  loadWasmModuleMock: vi.fn().mockResolvedValue({}) as any,
}));

vi.mock("./client.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./client.js")>();
  return {
    ...actual,
    loadWasmModule: loadWasmModuleMock,
  };
});

import { Db, type DbConfig } from "./db.js";
import { WasmRuntimeModule } from "./wasm-runtime-module.js";
import { LeaderMigratedError } from "./worker-bridge.js";

const originalWindow = (globalThis as Record<string, unknown>).window;
const originalLocation = globalThis.location;
const originalWorker = (globalThis as Record<string, unknown>).Worker;
const originalSharedWorker = (globalThis as Record<string, unknown>).SharedWorker;
const originalNavigator = (globalThis as Record<string, unknown>).navigator;

class FakeMessagePort extends EventTarget {
  readonly posted: unknown[] = [];

  postMessage(message: unknown): void {
    this.posted.push(message);
  }

  start(): void {}

  close(): void {}
}

class FakeSharedWorker {
  static readonly instances: FakeSharedWorker[] = [];

  readonly port = new FakeMessagePort();

  constructor(
    readonly url: string | URL,
    readonly options?: string | WorkerOptions,
  ) {
    FakeSharedWorker.instances.push(this);
  }
}

class FakeWorker {
  static readonly instances: FakeWorker[] = [];

  readonly posted: unknown[] = [];
  terminated = false;

  constructor(
    readonly url: string | URL,
    readonly options?: WorkerOptions,
  ) {
    FakeWorker.instances.push(this);
  }

  postMessage(message: unknown): void {
    this.posted.push(message);
  }

  addEventListener(): void {}
  removeEventListener(): void {}

  terminate(): void {
    this.terminated = true;
  }
}

/**
 * Stub `navigator.locks` that grants the lock immediately. The supervisor's
 * holdWhile callback runs in the same microtask flush, so by the time we
 * await the next microtask the dedicated worker has been spawned and
 * `state.endpoint` is set. The callback resolves only when the supervisor
 * voluntarily steps down (i.e., on `db.shutdown()`), mirroring the real
 * Web Locks contract.
 */
function installNavigatorLocksStub(): void {
  Object.defineProperty(globalThis, "navigator", {
    value: {
      locks: {
        async request(
          _name: string,
          _options: unknown,
          callback: () => Promise<unknown>,
        ): Promise<unknown> {
          return await callback();
        },
      },
      storage: undefined as
        | undefined
        | { getDirectory(): Promise<{ removeEntry: (n: string) => Promise<void> }> },
    },
    configurable: true,
  });
}

function installOpfsStub(removedEntries: string[]): void {
  const nav = (globalThis as { navigator: { storage?: unknown } }).navigator;
  nav.storage = {
    async getDirectory() {
      return {
        async removeEntry(name: string) {
          removedEntries.push(name);
        },
      };
    },
  };
}

async function createSharedWorkerDb(config: DbConfig): Promise<Db> {
  const runtimeModule = new WasmRuntimeModule();
  await runtimeModule.load(config);
  return await Db.createWithSharedWorker(config, runtimeModule);
}

afterEach(() => {
  loadWasmModuleMock.mockClear();
  FakeSharedWorker.instances.length = 0;
  FakeWorker.instances.length = 0;

  if (originalWindow === undefined) {
    delete (globalThis as Record<string, unknown>).window;
  } else {
    (globalThis as Record<string, unknown>).window = originalWindow;
  }
  if (originalLocation === undefined) {
    delete (globalThis as Record<string, unknown>).location;
  } else {
    (globalThis as Record<string, unknown>).location = originalLocation;
  }
  if (originalWorker === undefined) {
    delete (globalThis as Record<string, unknown>).Worker;
  } else {
    (globalThis as Record<string, unknown>).Worker = originalWorker;
  }
  if (originalSharedWorker === undefined) {
    delete (globalThis as Record<string, unknown>).SharedWorker;
  } else {
    (globalThis as Record<string, unknown>).SharedWorker = originalSharedWorker;
  }
  if (originalNavigator === undefined) {
    delete (globalThis as Record<string, unknown>).navigator;
  } else {
    Object.defineProperty(globalThis, "navigator", {
      value: originalNavigator,
      configurable: true,
    });
  }
});

describe("Db leader-tab runtime bootstrap", () => {
  it("spawns the broker SharedWorker and the leader's dedicated runtime Worker with the configured URLs", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;
    (globalThis as Record<string, unknown>).SharedWorker = FakeSharedWorker;
    installNavigatorLocksStub();

    const db = await createSharedWorkerDb({
      appId: "leader-tab-bootstrap-explicit-urls",
      driver: { type: "persistent", dbName: "leader-tab-bootstrap-explicit-urls" },
      runtimeSources: {
        baseUrl: "/ignored/",
        sharedWorkerUrl: "/custom/jazz-shared-worker.js",
        workerUrl: "/custom/jazz-worker.js",
        wasmUrl: "/custom/jazz_wasm_bg.wasm",
      },
    });

    await db.shutdown();

    expect(FakeSharedWorker.instances).toHaveLength(1);
    const broker = FakeSharedWorker.instances[0]!;
    const brokerUrl = new URL(String(broker.url));
    expect(brokerUrl.origin + brokerUrl.pathname).toBe(
      "http://localhost:3000/custom/jazz-shared-worker.js",
    );
    expect(broker.options).toEqual({
      type: "module",
      name: "jazz:leader-tab-bootstrap-explicit-urls:leader-tab-bootstrap-explicit-urls",
    });

    expect(FakeWorker.instances).toHaveLength(1);
    const runtimeWorker = FakeWorker.instances[0]!;
    expect(String(runtimeWorker.url)).toBe("http://localhost:3000/custom/jazz-worker.js");
    expect(runtimeWorker.options).toEqual({
      type: "module",
      name: "jazz-runtime:leader-tab-bootstrap-explicit-urls",
    });
    expect(runtimeWorker.terminated).toBe(true);
  });

  it("tears down the leader's dedicated runtime Worker and deletes the OPFS namespace on deleteClientStorage", async () => {
    const removedEntries: string[] = [];

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;
    (globalThis as Record<string, unknown>).SharedWorker = FakeSharedWorker;
    installNavigatorLocksStub();
    installOpfsStub(removedEntries);

    const db = await createSharedWorkerDb({
      appId: "leader-tab-reset",
      driver: { type: "persistent", dbName: "leader-tab-reset-db" },
      runtimeSources: {
        sharedWorkerUrl: "/custom/jazz-shared-worker.js",
        wasmSource: new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
      },
    });

    expect(FakeWorker.instances).toHaveLength(1);
    const runtimeWorker = FakeWorker.instances[0]!;

    await db.deleteClientStorage();

    expect(runtimeWorker.terminated).toBe(true);
    expect(removedEntries).toEqual(["leader-tab-reset-db.opfsbtree"]);

    await db.shutdown();
  });

  it("latches SharedWorker mode at construction so transient null endpoints don't downgrade clients", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;
    (globalThis as Record<string, unknown>).SharedWorker = FakeSharedWorker;
    installNavigatorLocksStub();

    const db = await createSharedWorkerDb({
      appId: "leader-tab-latch",
      driver: { type: "persistent", dbName: "leader-tab-latch-db" },
      runtimeSources: {
        sharedWorkerUrl: "/custom/jazz-shared-worker.js",
        wasmSource: new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
      },
    });

    const internals = db as unknown as {
      usesSharedWorker: boolean;
      workerEndpoint: unknown;
      onSupervisorStateChange: (state: { role: string; endpoint: unknown }) => void;
    };

    expect(internals.usesSharedWorker).toBe(true);
    expect(internals.workerEndpoint).not.toBeNull();

    // Simulate a leader handoff window: supervisor publishes endpoint=null.
    internals.onSupervisorStateChange({ role: "none", endpoint: null });
    expect(internals.workerEndpoint).toBeNull();

    // The mode latch survives the transient null so `getClient` still
    // builds worker-backed clients and skips its own `connectTransport`.
    expect(internals.usesSharedWorker).toBe(true);

    await db.shutdown();
  });

  it("ensureQueryReady rejects with LeaderMigratedError when a handoff never produces a fresh endpoint", async () => {
    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;
    (globalThis as Record<string, unknown>).SharedWorker = FakeSharedWorker;
    installNavigatorLocksStub();

    const db = await createSharedWorkerDb({
      appId: "leader-tab-stuck-handoff",
      driver: { type: "persistent", dbName: "leader-tab-stuck-handoff-db" },
      runtimeSources: {
        sharedWorkerUrl: "/custom/jazz-shared-worker.js",
        wasmSource: new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
      },
    });

    const internals = db as unknown as {
      onSupervisorStateChange: (state: { role: string; endpoint: unknown }) => void;
      ensureQueryReady: (options?: { tier?: string }) => Promise<void>;
    };

    // Push the supervisor into a stuck null-endpoint state (leader gone,
    // no replacement publishing). `ensureQueryReady` should not silently
    // succeed — it must wait for a fresh bridge and eventually surface a
    // typed `LeaderMigratedError` if one never arrives.
    internals.onSupervisorStateChange({ role: "none", endpoint: null });

    vi.useFakeTimers();
    let caught: unknown;
    try {
      // Pre-attach the catch so the rejection doesn't transit through
      // node's unhandled-rejection bookkeeping before the test observes it.
      const settled = internals.ensureQueryReady({ tier: "local" }).then(
        () => undefined,
        (error: unknown) => {
          caught = error;
        },
      );
      // Yield once so the inner subscribe registers before we advance.
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(20_000);
      await settled;
    } finally {
      vi.useRealTimers();
    }
    expect(caught).toBeInstanceOf(LeaderMigratedError);

    await db.shutdown();
  });
});

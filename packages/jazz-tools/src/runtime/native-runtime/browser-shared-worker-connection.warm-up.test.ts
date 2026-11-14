import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resolveBrowserWorkerRuntimeSources } from "../browser-worker-config.js";

// A fresh module per test: warm-ups are tracked per page, in module state.
let connection: typeof import("./browser-shared-worker-connection.js");

type Sent = { type: string; attemptId?: string; dbName?: string; [key: string]: unknown };

/**
 * One port per SharedWorker construction, answering like the broker's
 * bootstrap: probes are answered alive, a lease request is granted, a store
 * delete reports `deleteOutcome`.
 */
class FakeBootstrapPort {
  private readonly listeners = new Set<(event: MessageEvent) => void>();
  readonly sent: Sent[] = [];
  closed = false;

  constructor(
    readonly realm: string,
    private readonly answers: { alive: boolean; deleteOutcome: "deleted" | "held" },
  ) {}

  start(): void {}

  close(): void {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.listeners.add(listener);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void): void {
    if (type === "message") this.listeners.delete(listener);
  }

  postMessage(message: Sent): void {
    this.sent.push(message);
    const answer = (data: unknown) => queueMicrotask(() => this.emit(data));
    if (message.type === "probe-foreground-node-lease-worker" && this.answers.alive) {
      answer({ type: "foreground-node-lease-worker-alive", attemptId: message.attemptId });
    } else if (message.type === "acquire-foreground-node-lease") {
      answer({
        type: "foreground-node-lease-ready",
        leaseId: "00000000-0000-4000-8000-000000000001",
        node: new Uint8Array(16),
        confirmedTxTime: "0",
      });
    } else if (message.type === "delete-store") {
      answer({ type: "delete-store-result", outcome: this.answers.deleteOutcome });
    } else if (message.type === "retire-foreground-node-lease") {
      answer({ type: "foreground-node-lease-result" });
    }
  }

  private emit(data: unknown): void {
    for (const listener of [...this.listeners]) listener({ data } as MessageEvent);
  }
}

function stubSharedWorkers(answers = { alive: true, deleteOutcome: "deleted" as const }) {
  const ports: FakeBootstrapPort[] = [];
  vi.stubGlobal(
    "SharedWorker",
    class {
      readonly port: FakeBootstrapPort;
      constructor(_url: URL | string, options: { name: string }) {
        this.port = new FakeBootstrapPort(options.name, answers);
        ports.push(this.port);
      }
    },
  );
  return ports;
}

function memoryLocalStorage() {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => void values.set(key, String(value)),
    removeItem: (key: string) => void values.delete(key),
    key: (index: number) => [...values.keys()][index] ?? null,
    get length() {
      return values.size;
    },
  };
}

const SOURCES = { wasmUrl: "https://assets.test/jazz.wasm", wasmVersion: "build-1" };
const ALICE_ROOT = "app::jazz-browser-v1::alice";
const BOB_ROOT = "app::jazz-browser-v1::bob";

function realmOf(dbName: string, sources: typeof SOURCES = SOURCES) {
  return `${connection.createBrowserSharedWorkerBaseName(resolveBrowserWorkerRuntimeSources(sources), dbName)}:generation-0`;
}

beforeEach(async () => {
  vi.resetModules();
  connection = await import("./browser-shared-worker-connection.js");
  vi.stubGlobal("localStorage", memoryLocalStorage());
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("warming the remembered broker worker before the account resolves", () => {
  it("only probes the worker, asking it to prefetch its WASM: no admission, no store", async () => {
    const ports = stubSharedWorkers();
    await expect(connection.warmBrowserWorker(SOURCES, ALICE_ROOT)).resolves.toBe(true);
    expect(ports.map((port) => port.realm)).toEqual([realmOf(ALICE_ROOT)]);
    expect(ports[0]!.sent).toEqual([
      {
        type: "probe-foreground-node-lease-worker",
        attemptId: expect.stringMatching(/^warm-/),
        wasmPrefetch: {
          runtimeSources: expect.objectContaining({
            wasmUrl: "https://assets.test/jazz.wasm?jazz-runtime-version=build-1",
          }),
        },
      },
    ]);
  });

  it("lets the lease for the same root take the realm, then releases its probe", async () => {
    const ports = stubSharedWorkers();
    await connection.warmBrowserWorker(SOURCES, ALICE_ROOT);
    const lease = await connection.SharedBrowserForegroundNodeLease.acquire({
      runtimeSources: SOURCES,
      dbName: ALICE_ROOT,
      storageOwner: "alice",
    });
    const [warm, leased] = ports;
    // The same realm: the lease attaches where the warm-up started the worker.
    expect(leased!.realm).toBe(warm!.realm);
    // The lease goes through its own full admission, on its own port.
    expect(leased!.sent.map((message) => message.type)).toEqual([
      "probe-foreground-node-lease-worker",
      "acquire-foreground-node-lease",
    ]);
    expect(leased!.sent[1]).toMatchObject({ dbName: ALICE_ROOT, storageOwner: "alice" });
    // The warm-up never asked for anything but the probe, and let go.
    expect(warm!.sent.map((message) => message.type)).toEqual([
      "probe-foreground-node-lease-worker",
      "cancel-foreground-node-lease",
    ]);
    expect(warm!.closed).toBe(true);
    expect(leased!.closed).toBe(false);
    await lease.retire();
  });

  it("leaves another account's warmed realm idle and lets it go after its hold", async () => {
    vi.useFakeTimers();
    const ports = stubSharedWorkers();
    // The remembered root is Alice's, but Bob signed in on this device since.
    const warming = connection.warmBrowserWorker(SOURCES, ALICE_ROOT);
    await vi.advanceTimersByTimeAsync(0);
    await expect(warming).resolves.toBe(true);
    const acquiring = connection.SharedBrowserForegroundNodeLease.acquire({
      runtimeSources: SOURCES,
      dbName: BOB_ROOT,
      storageOwner: "bob",
    });
    await vi.advanceTimersByTimeAsync(0);
    const lease = await acquiring;
    const [alice, bob] = ports;
    // Bob's root is a different realm: Alice's warmed worker serves him nothing.
    expect(bob!.realm).toBe(realmOf(BOB_ROOT));
    expect(alice!.realm).not.toBe(bob!.realm);
    expect(alice!.sent.map((message) => message.type)).toEqual([
      "probe-foreground-node-lease-worker",
    ]);
    await vi.advanceTimersByTimeAsync(connection.WARM_WORKER_HOLD_MS);
    expect(alice!.sent.map((message) => message.type)).toEqual([
      "probe-foreground-node-lease-worker",
      "cancel-foreground-node-lease",
    ]);
    expect(alice!.closed).toBe(true);
    const retired = lease.retire();
    await vi.advanceTimersByTimeAsync(0);
    await retired;
  });

  it("names the realm from the page's current assets, never remembered ones", async () => {
    const ports = stubSharedWorkers();
    const nextDeploy = { wasmUrl: "https://assets.test/jazz.wasm", wasmVersion: "build-2" };
    await connection.warmBrowserWorker(nextDeploy, ALICE_ROOT);
    expect(ports[0]!.realm).toBe(realmOf(ALICE_ROOT, nextDeploy));
    expect(ports[0]!.realm).not.toBe(realmOf(ALICE_ROOT));
  });

  it("starts one warm-up per realm", async () => {
    const ports = stubSharedWorkers();
    await connection.warmBrowserWorker(SOURCES, ALICE_ROOT);
    await connection.warmBrowserWorker(SOURCES, ALICE_ROOT);
    expect(ports).toHaveLength(1);
  });

  it("resolves false, without throwing, when no worker can be started", async () => {
    vi.stubGlobal(
      "SharedWorker",
      class {
        constructor() {
          throw new Error("SharedWorker is not available");
        }
      },
    );
    await expect(connection.warmBrowserWorker(SOURCES, ALICE_ROOT)).resolves.toBe(false);
  });
});

describe("the remembered browser root", () => {
  const registry = "https://app.test/jazz/apps/app/accounts";

  it("is kept per registry", () => {
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBeNull();
    connection.rememberBrowserWorkerRoot(registry, ALICE_ROOT);
    connection.rememberBrowserWorkerRoot("https://other.test/jazz/apps/app/accounts", BOB_ROOT);
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBe(ALICE_ROOT);
    connection.rememberBrowserWorkerRoot(registry, BOB_ROOT);
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBe(BOB_ROOT);
  });

  it("is forgotten once its store is deleted, and kept while the store is held", async () => {
    connection.rememberBrowserWorkerRoot(registry, ALICE_ROOT);
    stubSharedWorkers({ alive: true, deleteOutcome: "held" as never });
    await expect(connection.deleteBrowserStore(SOURCES, ALICE_ROOT)).resolves.toBe("held");
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBe(ALICE_ROOT);

    stubSharedWorkers();
    await expect(connection.deleteBrowserStore(SOURCES, BOB_ROOT)).resolves.toBe("deleted");
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBe(ALICE_ROOT);
    await expect(connection.deleteBrowserStore(SOURCES, ALICE_ROOT)).resolves.toBe("deleted");
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBeNull();
  });
});

describe("the default runtime source's lease", () => {
  const base64Url = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const jwt = `${base64Url({ alg: "none" })}.${base64Url({ sub: "subject", iss: "https://issuer.test" })}.signature`;

  it("remembers the root it leased under its account registry, for the next load", async () => {
    vi.stubGlobal("window", {});
    vi.stubGlobal("Worker", class {});
    stubSharedWorkers();
    const { DefaultRuntimeSource } = await import("../default-runtime-source.js");
    const acquire = vi.spyOn(connection.SharedBrowserForegroundNodeLease, "acquire");
    const registry = "https://registry.test/apps/early-lease-app/accounts";
    const lease = await new DefaultRuntimeSource().acquireBrowserForegroundNodeLease({
      appId: "early-lease-app",
      accountId: "00000000-0000-4000-8000-0000000000a1",
      accountRegistryAuthority: registry,
      jwtToken: jwt,
      driver: { type: "persistent" },
      runtimeSources: SOURCES,
    });
    const leasedRoot = acquire.mock.calls[0]![0].dbName;
    expect(connection.rememberedBrowserWorkerRoot(registry)).toBe(leasedRoot);
    await lease.retire();
  });
});

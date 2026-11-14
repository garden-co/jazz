import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import { clientRegistryUrl } from "../accounts/account-manager-core.js";
import type { LocalFirstModule } from "../runtime/local-first-upgrade.js";
import type { LocalFirstWarmUpConfig } from "../runtime/local-first-warm.js";
import { openBrowserSessionCore as createJazzClient } from "./create-jazz-session.js";

const SERVER_URL = "http://127.0.0.1:9/jazz";

function recordingStore(events: string[]): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      events.push("account-manager");
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
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

/** A local-first module whose client never opens: these tests end at startup. */
function localFirstModule(
  events: string[],
  warmUp: (config: LocalFirstWarmUpConfig) => Promise<void> = async () => undefined,
) {
  const calls: LocalFirstWarmUpConfig[] = [];
  const module: LocalFirstModule = {
    warmUp: async (config) => {
      events.push("warm-up");
      calls.push(config);
      await warmUp(config);
    },
    createClient: async () => {
      events.push("create-client");
      throw new Error("synthetic local-first open failure");
    },
  };
  return { loader: async () => module, calls };
}

beforeEach(() => {
  vi.stubGlobal("window", { document: {} });
  vi.stubGlobal("document", {});
  vi.stubGlobal("localStorage", memoryLocalStorage());
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("the local-first warm-up before a browser session resolves its account", () => {
  it("starts before the account manager when the session opens local-first now", async () => {
    const events: string[] = [];
    const { loader, calls } = localFirstModule(events);
    await expect(
      createJazzClient({
        serverUrl: SERVER_URL,
        store: recordingStore(events),
        initial: "device",
        localFirst: "now",
        runtimeSources: { wasmUrl: "https://assets.test/jazz.wasm" },
        client: loader,
      }),
    ).rejects.toThrow("synthetic local-first open failure");
    expect(events.indexOf("warm-up")).toBeGreaterThanOrEqual(0);
    expect(events.indexOf("warm-up")).toBeLessThan(events.indexOf("account-manager"));
    expect(calls).toEqual([
      {
        registry: clientRegistryUrl({ serverUrl: SERVER_URL }),
        runtimeSources: { wasmUrl: "https://assets.test/jazz.wasm" },
        driver: undefined,
        worker: true,
      },
    ]);
  });

  it("starts no worker of the last user's on a shared device (clearOnSignOut)", async () => {
    const events: string[] = [];
    const { loader, calls } = localFirstModule(events);
    await expect(
      createJazzClient({
        serverUrl: SERVER_URL,
        store: recordingStore(events),
        initial: "device",
        localFirst: "now",
        clearOnSignOut: true,
        client: loader,
      }),
    ).rejects.toThrow();
    expect(calls).toHaveLength(1);
    expect(calls[0]!.worker).toBe(false);
  });

  it("warms nothing for a background session whose registry never upgraded", async () => {
    const events: string[] = [];
    const { loader, calls } = localFirstModule(events);
    // Background, never upgraded: the session runs thin and never resolves
    // its first backend answer here, so only its start is observed.
    void createJazzClient({
      serverUrl: SERVER_URL,
      store: recordingStore(events),
      initial: "device",
      client: loader,
    }).catch(() => undefined);
    await vi.waitFor(() => expect(events).toContain("account-manager"));
    expect(calls).toEqual([]);
  });

  it("warms a background session whose registry upgraded before", async () => {
    const registry = clientRegistryUrl({ serverUrl: SERVER_URL });
    localStorage.setItem(
      `jazz-local-first-v1:${encodeURIComponent(registry)}`,
      JSON.stringify(["some-account"]),
    );
    const events: string[] = [];
    const { loader, calls } = localFirstModule(events);
    void createJazzClient({
      serverUrl: SERVER_URL,
      store: recordingStore(events),
      initial: "device",
      client: loader,
    }).catch(() => undefined);
    await vi.waitFor(() => expect(events).toContain("account-manager"));
    expect(events.indexOf("warm-up")).toBeLessThan(events.indexOf("account-manager"));
    expect(calls).toHaveLength(1);
  });

  it("waits for a slow warm-up only for its budget", async () => {
    const events: string[] = [];
    const { loader } = localFirstModule(events, () => new Promise<void>(() => {}));
    const started = Date.now();
    await expect(
      createJazzClient({
        serverUrl: SERVER_URL,
        store: recordingStore(events),
        initial: "device",
        localFirst: "now",
        client: loader,
      }),
    ).rejects.toThrow("synthetic local-first open failure");
    expect(events.slice(0, 2)).toEqual(["warm-up", "account-manager"]);
    expect(Date.now() - started).toBeLessThan(5_000);
  });

  it("opens as before when the warm-up fails or the loader has none", async () => {
    const events: string[] = [];
    const failing = localFirstModule(events, async () => {
      throw new Error("warm-up failed");
    });
    await expect(
      createJazzClient({
        serverUrl: SERVER_URL,
        store: recordingStore(events),
        initial: "device",
        localFirst: "now",
        client: failing.loader,
      }),
    ).rejects.toThrow("synthetic local-first open failure");
    expect(events).toContain("create-client");

    const withoutWarmUp: LocalFirstModule = {
      createClient: async () => {
        throw new Error("synthetic local-first open failure");
      },
    };
    await expect(
      createJazzClient({
        serverUrl: SERVER_URL,
        store: recordingStore([]),
        initial: "device",
        localFirst: "now",
        client: async () => withoutWarmUp,
      }),
    ).rejects.toThrow("synthetic local-first open failure");
  });
});

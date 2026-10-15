import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { tokenGate } = vi.hoisted(() => ({
  tokenGate: { next: null as Promise<void> | null },
}));

vi.mock("./enrollment.js", async (importOriginal) => {
  const original = await importOriginal<typeof import("./enrollment.js")>();
  return {
    ...original,
    accountToken: async (...args: Parameters<typeof original.accountToken>) => {
      const gate = tokenGate.next;
      tokenGate.next = null;
      if (gate) await gate;
      return original.accountToken(...args);
    },
  };
});

import { createAccountDbWithRuntimeSource } from "./context.js";
import { createAccountManager } from "./create-account-manager.js";
import { Db, type DbConfig } from "../runtime/db.js";
import type { BrowserForegroundNodeLease, RuntimeSource } from "../runtime/runtime-source.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function fakeRuntimeSource() {
  const events: string[] = [];
  const lease = {
    node: new Uint8Array(16),
    confirmedTxTime: 3n,
    returnWithHighWater: vi.fn(async () => {
      events.push("lease-returned");
    }),
  } as unknown as BrowserForegroundNodeLease;
  const raw = {
    supportsBrowserWorker: true,
    load: vi.fn(async () => {
      events.push("runtime-load");
    }),
    admitConfig: vi.fn(),
    shutdown: vi.fn(async () => undefined),
    acquireBrowserForegroundNodeLease: vi.fn(async () => {
      events.push("lease-acquired");
      return lease;
    }),
  };
  return { source: raw as unknown as RuntimeSource<DbConfig>, raw, lease, events };
}

async function localAccount() {
  const appId = `early-lease-${crypto.randomUUID()}`;
  let saved: string | null = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl: "http://127.0.0.1:1",
    store: {
      async read() {
        return saved;
      },
      async update(transform) {
        saved = transform(saved);
      },
    },
  });
  return { appId, account: accounts.createLocalFirst() };
}

let fixture: Awaited<ReturnType<typeof localAccount>>;

beforeEach(async () => {
  fixture = await localAccount();
  // Browser-shaped globals only after the account manager has loaded its runtime.
  vi.stubGlobal("window", {});
  vi.stubGlobal("Worker", class {});
});

afterEach(() => {
  tokenGate.next = null;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("account context early worker lease", () => {
  it("boots the worker while the account credential resolves and hands it to the Db", async () => {
    const { source, raw, lease, events } = fakeRuntimeSource();
    const create = vi.spyOn(Db, "createWithBrowserWorker").mockResolvedValue({
      onAuthChanged: () => () => undefined,
      onShutdown: () => undefined,
    } as unknown as Db);
    const credential = deferred<void>();
    tokenGate.next = credential.promise;

    const opening = createAccountDbWithRuntimeSource(
      { appId: fixture.appId, account: fixture.account, driver: { type: "persistent" } },
      source,
    );
    await vi.waitFor(() => expect(events).toEqual(["lease-acquired"]));
    // The credential has not resolved and the page runtime has not loaded.
    expect(raw.load).not.toHaveBeenCalled();

    credential.resolve();
    await opening;
    expect(raw.acquireBrowserForegroundNodeLease).toHaveBeenCalledOnce();
    await expect(create.mock.calls[0]![2]).resolves.toBe(lease);
    expect(lease.returnWithHighWater).not.toHaveBeenCalled();
  });

  it("returns the early lease when the account credential cannot be resolved", async () => {
    const { source, raw, lease, events } = fakeRuntimeSource();
    const create = vi.spyOn(Db, "createWithBrowserWorker");
    tokenGate.next = Promise.reject(new Error("credential unavailable"));

    await expect(
      createAccountDbWithRuntimeSource(
        { appId: fixture.appId, account: fixture.account, driver: { type: "persistent" } },
        source,
      ),
    ).rejects.toThrow("credential unavailable");

    expect(create).not.toHaveBeenCalled();
    expect(lease.returnWithHighWater).toHaveBeenCalledExactlyOnceWith(3n);
    // The lease is back before the runtime source is torn down.
    expect(events).toEqual(["lease-acquired", "lease-returned"]);
    expect(raw.shutdown).toHaveBeenCalledOnce();
  });
});

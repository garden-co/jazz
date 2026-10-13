import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../accounts/config-capability.js", () => ({
  assertAccountConfig: () => undefined,
  copyAccountConfigAdmission: () => undefined,
}));

import { createDbWithRuntimeSource, Db, startBrowserWorkerLease, type DbConfig } from "./db.js";
import type { BrowserForegroundNodeLease, RuntimeSource } from "./runtime-source.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const base64Url = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
const jwt = `${base64Url({ alg: "none" })}.${base64Url({ sub: "subject", iss: "https://issuer.test" })}.signature`;

function accountConfig(accountId = "00000000-0000-4000-8000-0000000000a1"): DbConfig {
  return {
    appId: "early-lease-app",
    accountId,
    accountRegistryAuthority: "https://registry.test",
    jwtToken: jwt,
    driver: { type: "persistent" },
  } as DbConfig;
}

function fakeLease(): BrowserForegroundNodeLease {
  return {
    node: new Uint8Array(16),
    confirmedTxTime: 7n,
    returnWithHighWater: vi.fn(async () => undefined),
  } as unknown as BrowserForegroundNodeLease;
}

function fakeRuntimeSource(load: Promise<void>) {
  const events: string[] = [];
  const lease = fakeLease();
  const source = {
    supportsBrowserWorker: true,
    load: vi.fn(async () => {
      events.push("load-start");
      await load;
      events.push("load-end");
    }),
    admitConfig: vi.fn(),
    acquireBrowserForegroundNodeLease: vi.fn(async () => {
      events.push("lease");
      return lease;
    }),
  };
  return { source: source as unknown as RuntimeSource<DbConfig>, events, lease, raw: source };
}

beforeEach(() => {
  vi.stubGlobal("window", {});
  vi.stubGlobal("Worker", class {});
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("early browser worker lease", () => {
  it("starts the worker lease before the page runtime finishes loading and adopts it", async () => {
    const load = deferred<void>();
    const { source, events, lease, raw } = fakeRuntimeSource(load.promise);
    const createdDb = {} as Db;
    const create = vi.spyOn(Db, "createWithBrowserWorker").mockResolvedValue(createdDb);

    const opening = createDbWithRuntimeSource(accountConfig(), source);
    await vi.waitFor(() => expect(events).toContain("load-start"));
    // The worker is already being admitted while the runtime is still loading.
    expect(events).toEqual(["lease", "load-start"]);

    load.resolve();
    await expect(opening).resolves.toBe(createdDb);
    expect(raw.acquireBrowserForegroundNodeLease).toHaveBeenCalledOnce();
    await expect(create.mock.calls[0]![2]).resolves.toBe(lease);
    expect(lease.returnWithHighWater).not.toHaveBeenCalled();
  });

  it("adopts a lease started even earlier by the caller", async () => {
    const { source, raw, lease } = fakeRuntimeSource(Promise.resolve());
    const create = vi.spyOn(Db, "createWithBrowserWorker").mockResolvedValue({} as Db);
    const started = startBrowserWorkerLease(accountConfig(), source)!;

    await createDbWithRuntimeSource(accountConfig(), source, started);

    expect(raw.acquireBrowserForegroundNodeLease).toHaveBeenCalledOnce();
    expect(create.mock.calls[0]![2]).toBe(started);
    expect(lease.returnWithHighWater).not.toHaveBeenCalled();
  });

  it("returns an unadopted lease when the runtime fails to load", async () => {
    const { source, lease } = fakeRuntimeSource(Promise.reject(new Error("runtime load failed")));
    const create = vi.spyOn(Db, "createWithBrowserWorker");

    await expect(createDbWithRuntimeSource(accountConfig(), source)).rejects.toThrow(
      "runtime load failed",
    );
    expect(create).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(lease.returnWithHighWater).toHaveBeenCalledWith(7n));
  });

  it("returns a caller's lease for a different root before acquiring the matching one", async () => {
    const { source, raw, lease } = fakeRuntimeSource(Promise.resolve());
    const returned = deferred<void>();
    vi.mocked(lease.returnWithHighWater).mockReturnValueOnce(returned.promise);
    const create = vi.spyOn(Db, "createWithBrowserWorker").mockResolvedValue({} as Db);
    const otherAccount = startBrowserWorkerLease(
      accountConfig("00000000-0000-4000-8000-0000000000b2"),
      source,
    )!;

    const opening = createDbWithRuntimeSource(accountConfig(), source, otherAccount);
    await vi.waitFor(() => expect(lease.returnWithHighWater).toHaveBeenCalledOnce());
    // The Db (and so its own lease acquisition) waits for that return.
    await Promise.resolve();
    expect(create).not.toHaveBeenCalled();

    returned.resolve();
    await opening;
    expect(create.mock.calls[0]![2]).toBeUndefined();
    expect(lease.returnWithHighWater).toHaveBeenCalledOnce();
    expect(raw.acquireBrowserForegroundNodeLease).toHaveBeenCalledOnce();
  });

  it("does not start a worker for a credential-scoped (non-account) root", async () => {
    const { source, raw } = fakeRuntimeSource(Promise.resolve());
    vi.spyOn(Db, "createWithBrowserWorker").mockResolvedValue({} as Db);

    await createDbWithRuntimeSource(
      { appId: "early-lease-app", jwtToken: jwt, driver: { type: "persistent" } } as DbConfig,
      source,
    );

    // The connection manager acquires it itself, after auth has resolved.
    expect(raw.acquireBrowserForegroundNodeLease).not.toHaveBeenCalled();
  });

  it("does not start a worker outside the browser", async () => {
    vi.unstubAllGlobals();
    const { source, raw } = fakeRuntimeSource(Promise.resolve());
    vi.spyOn(Db, "createWithDirectConnection").mockResolvedValue({} as Db);

    await createDbWithRuntimeSource(accountConfig(), source);

    expect(raw.acquireBrowserForegroundNodeLease).not.toHaveBeenCalled();
  });
});

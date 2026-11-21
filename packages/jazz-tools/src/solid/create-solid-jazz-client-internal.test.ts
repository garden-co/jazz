import {
  GracefulShutdownSyncError,
  SharedClientShutdownError,
} from "../runtime/graceful-shutdown-error.js";
import { createRoot, createSignal } from "solid-js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { AccountDbConfig } from "../accounts/context.js";
import { accountConfig } from "../../tests/shared/account-config.js";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function flushMicrotasks(): Promise<void> {
  for (let i = 0; i < 8; i++) await Promise.resolve();
}

import {
  JazzClientFactory,
  createSolidJazzClientInternal,
} from "./create-solid-jazz-client-internal.js";

function makeRawClient(id: string, shutdown = vi.fn(async (): Promise<void> => {})) {
  return {
    id,
    db: { clientId: id } as any,
    session: null,
    shutdown,
  };
}

describe("solid/createJazzClientInternal solid-js lifecycle", () => {
  let defaultClientFactory: ReturnType<typeof vi.fn<JazzClientFactory>>;

  beforeEach(() => {
    defaultClientFactory = vi.fn<JazzClientFactory>();
  });

  it.each([
    new GracefulShutdownSyncError(new Error("sync unavailable")),
    new SharedClientShutdownError(),
  ])("forwards graceful shutdown and retains the open client after %s", async (failure) => {
    const shutdown = vi.fn().mockRejectedValueOnce(failure).mockResolvedValue(undefined);
    const raw = makeRawClient("graceful", shutdown);
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;
    try {
      createRoot((stop) => {
        dispose = stop;
        result = createSolidJazzClientInternal(
          () => accountConfig("graceful"),
          async () => raw,
        );
      });
      await flushMicrotasks();
      const client = result.client!;
      await expect(client.shutdown({ waitForSync: true })).rejects.toBe(failure);
      expect(shutdown).toHaveBeenCalledWith({ waitForSync: true });
      expect(result.client).toBe(client);
      await client.shutdown({ waitForSync: true });
      expect(result.client).toBeUndefined();
    } finally {
      dispose?.();
    }
  });

  it("hides the client after a terminal teardown failure", async () => {
    const failure = new Error("storage teardown failed");
    const shutdown = vi.fn().mockRejectedValueOnce(failure).mockResolvedValue(undefined);
    const raw = makeRawClient("terminal", shutdown);
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;
    try {
      createRoot((stop) => {
        dispose = stop;
        result = createSolidJazzClientInternal(
          () => accountConfig("terminal"),
          async () => raw,
        );
      });
      await flushMicrotasks();
      await expect(result.client!.shutdown({ waitForSync: true })).rejects.toBe(failure);
      expect(result.client).toBeUndefined();
    } finally {
      dispose?.();
    }
  });

  it("SD-LIFE-01: on rapid reconfig, discards stale client and keeps newest run active", async () => {
    const pendingByAppId = new Map<AccountDbConfig["appId"], Deferred<any>>();

    defaultClientFactory.mockImplementation((config: AccountDbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: AccountDbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<AccountDbConfig>(accountConfig("A"));
      setConfig = _setConfig;
      result = createSolidJazzClientInternal(config, defaultClientFactory);
      return undefined;
    });

    await flushMicrotasks();

    expect(defaultClientFactory).toHaveBeenCalledTimes(1);
    expect(defaultClientFactory).toHaveBeenNthCalledWith(1, accountConfig("A"));

    setConfig(accountConfig("B"));
    await flushMicrotasks();

    expect(defaultClientFactory).toHaveBeenCalledTimes(1);

    const staleShutdown = deferred<void>();
    const rawA = makeRawClient(
      "A",
      vi.fn(() => staleShutdown.promise),
    );
    pendingByAppId.get("A")!.resolve(rawA);
    await flushMicrotasks();

    expect(rawA.shutdown).toHaveBeenCalledTimes(1);
    expect(result.loading).toBe(true);
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);

    staleShutdown.resolve();
    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(2);
    expect(defaultClientFactory).toHaveBeenNthCalledWith(2, accountConfig("B"));

    const rawB = makeRawClient("B");
    pendingByAppId.get("B")!.resolve(rawB);
    await flushMicrotasks();

    expect(result.loading).toBe(false);
    expect(result.client).toBeDefined();
    expect(result.client).toMatchObject({ id: "B" });

    dispose();
    await flushMicrotasks();

    expect(rawB.shutdown).toHaveBeenCalledTimes(1);
  });

  it("SD-LIFE-02: if unmounted while pending, shuts down client after late resolve", async () => {
    const pendingA = deferred<any>();

    defaultClientFactory.mockImplementation(() => pendingA.promise);

    let dispose!: () => void;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      createSolidJazzClientInternal(() => accountConfig("A"), defaultClientFactory);
      return undefined;
    });

    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);

    dispose();

    const rawA = makeRawClient("A");
    pendingA.resolve(rawA);
    await flushMicrotasks();

    expect(rawA.shutdown).toHaveBeenCalledTimes(1);
  });

  it("SD-LIFE-03: during reconfig, hides previous client until next client resolves", async () => {
    const pendingByAppId = new Map<AccountDbConfig["appId"], Deferred<any>>();

    defaultClientFactory.mockImplementation((config: AccountDbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: AccountDbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<AccountDbConfig>(accountConfig("A"));
      setConfig = _setConfig;
      result = createSolidJazzClientInternal(config, defaultClientFactory);
      return undefined;
    });

    await flushMicrotasks();

    const rawA = makeRawClient("A");
    pendingByAppId.get("A")!.resolve(rawA);
    await flushMicrotasks();

    expect(result.client).toBeDefined();
    expect(result.client).toMatchObject({ id: "A" });

    setConfig(accountConfig("B"));
    await flushMicrotasks();

    // During B pending, current client should be unavailable (cutover/loading).
    expect(result.loading).toBe(true);
    expect(result.client).toBeUndefined();

    const rawB = makeRawClient("B");
    pendingByAppId.get("B")!.resolve(rawB);
    await flushMicrotasks();

    expect(result.client).toBeDefined();
    expect(result.client).toMatchObject({ id: "B" });

    dispose();
    await flushMicrotasks();
  });

  it("SD-LIFE-04: uses provided clientFactory to create clients", async () => {
    const clientFactory = vi.fn(async () => makeRawClient("factory"));
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      result = createSolidJazzClientInternal(() => accountConfig("A"), clientFactory);
      return undefined;
    });

    await flushMicrotasks();

    expect(clientFactory).toHaveBeenCalledTimes(1);
    expect(clientFactory).toHaveBeenCalledWith(accountConfig("A"));
    expect(result.client).toBeDefined();
    expect(result.client).toMatchObject({ id: "factory" });

    dispose();
    await flushMicrotasks();
  });

  it("SD-LIFE-05: preserves client for equivalent config and recreates only on semantic config change", async () => {
    const pendingByAppId = new Map<AccountDbConfig["appId"], Deferred<any>>();

    defaultClientFactory.mockImplementation((config: AccountDbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: AccountDbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<AccountDbConfig>(accountConfig("A"));
      setConfig = _setConfig;
      result = createSolidJazzClientInternal(config, defaultClientFactory);
      return undefined;
    });

    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);

    const rawA = makeRawClient("A");
    pendingByAppId.get("A")!.resolve(rawA);
    await flushMicrotasks();

    expect(result.client).toMatchObject({ id: "A" });

    // Equivalent config object must not recreate the client.
    setConfig(accountConfig("A"));
    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);
    expect(rawA.shutdown).toHaveBeenCalledTimes(0);
    expect(result.client).toMatchObject({ id: "A" });

    // Semantically different config should recreate the client.
    setConfig(accountConfig("B"));
    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(2);
    expect(rawA.shutdown).toHaveBeenCalledTimes(1);

    const rawB = makeRawClient("B");
    pendingByAppId.get("B")!.resolve(rawB);
    await flushMicrotasks();

    expect(result.client).toMatchObject({ id: "B" });

    dispose();
    await flushMicrotasks();
    expect(rawB.shutdown).toHaveBeenCalledTimes(1);
  });

  it("SD-LIFE-06: exposes shutdown failure to the current replacement and lets a later config recover", async () => {
    const pendingByAppId = new Map<AccountDbConfig["appId"], Deferred<any>>();
    defaultClientFactory.mockImplementation((config: AccountDbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: AccountDbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<AccountDbConfig>(accountConfig("A"));
      setConfig = _setConfig;
      result = createSolidJazzClientInternal(config, defaultClientFactory);
      return undefined;
    });

    await flushMicrotasks();
    const failedShutdown = deferred<void>();
    const rawA = makeRawClient(
      "A",
      vi.fn(() => failedShutdown.promise),
    );
    pendingByAppId.get("A")!.resolve(rawA);
    await flushMicrotasks();

    setConfig(accountConfig("B"));
    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);

    const shutdownError = new Error("A did not shut down");
    failedShutdown.reject(shutdownError);
    await flushMicrotasks();

    expect(result.error).toBe(shutdownError);
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);

    setConfig(accountConfig("C"));
    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(2);
    expect(defaultClientFactory).toHaveBeenLastCalledWith(accountConfig("C"));

    const rawC = makeRawClient("C");
    pendingByAppId.get("C")!.resolve(rawC);
    await flushMicrotasks();
    expect(result.error).toBeUndefined();
    expect(result.client).toMatchObject({ id: "C" });

    dispose();
    await flushMicrotasks();
  });
});

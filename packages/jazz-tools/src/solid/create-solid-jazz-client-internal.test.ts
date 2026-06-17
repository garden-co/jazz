import { createRoot, createSignal } from "solid-js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DbConfig } from "../runtime/db.js";

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
  await Promise.resolve();
}

import {
  JazzClientFactory,
  createSolidJazzClientInternal,
} from "./create-solid-jazz-client-internal.js";

function makeRawClient(id: string) {
  return {
    id,
    db: { clientId: id } as any,
    session: null,
    manager: {} as any,
    shutdown: vi.fn(async () => undefined),
  };
}

describe("solid/createJazzClientInternal solid-js lifecycle", () => {
  let defaultClientFactory: ReturnType<typeof vi.fn<JazzClientFactory>>;

  beforeEach(() => {
    defaultClientFactory = vi.fn<JazzClientFactory>();
  });

  it("SD-LIFE-01: on rapid reconfig, discards stale client and keeps newest run active", async () => {
    const pendingByAppId = new Map<string, Deferred<any>>();

    defaultClientFactory.mockImplementation((config: DbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: DbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<DbConfig>({ appId: "A" });
      setConfig = _setConfig;
      result = createSolidJazzClientInternal(config, defaultClientFactory);
      return undefined;
    });

    await flushMicrotasks();

    expect(defaultClientFactory).toHaveBeenCalledTimes(1);
    expect(defaultClientFactory).toHaveBeenNthCalledWith(1, {
      appId: "A",
    });

    setConfig({ appId: "B" });
    await flushMicrotasks();

    expect(defaultClientFactory).toHaveBeenCalledTimes(2);
    expect(defaultClientFactory).toHaveBeenNthCalledWith(2, {
      appId: "B",
    });

    const rawA = makeRawClient("A");
    pendingByAppId.get("A")!.resolve(rawA);
    await flushMicrotasks();

    expect(rawA.shutdown).toHaveBeenCalledTimes(1);
    expect(result.loading).toBe(true);

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
      createSolidJazzClientInternal(() => ({ appId: "A" }), defaultClientFactory);
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
    const pendingByAppId = new Map<string, Deferred<any>>();

    defaultClientFactory.mockImplementation((config: DbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: DbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<DbConfig>({ appId: "A" });
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

    setConfig({ appId: "B" });
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
      result = createSolidJazzClientInternal(() => ({ appId: "A" }), clientFactory);
      return undefined;
    });

    await flushMicrotasks();

    expect(clientFactory).toHaveBeenCalledTimes(1);
    expect(clientFactory).toHaveBeenCalledWith({ appId: "A" });
    expect(result.client).toBeDefined();
    expect(result.client).toMatchObject({ id: "factory" });

    dispose();
    await flushMicrotasks();
  });

  it("SD-LIFE-05: preserves client for equivalent config and recreates only on semantic config change", async () => {
    const pendingByAppId = new Map<string, Deferred<any>>();

    defaultClientFactory.mockImplementation((config: DbConfig) => {
      const entry = deferred<any>();
      pendingByAppId.set(config.appId, entry);
      return entry.promise;
    });

    let setConfig!: (next: DbConfig) => void;
    let dispose!: () => void;
    let result!: ReturnType<typeof createSolidJazzClientInternal>;

    createRoot((rootDispose) => {
      dispose = rootDispose;
      const [config, _setConfig] = createSignal<DbConfig>({ appId: "A" });
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
    setConfig({ appId: "A" });
    await flushMicrotasks();
    expect(defaultClientFactory).toHaveBeenCalledTimes(1);
    expect(rawA.shutdown).toHaveBeenCalledTimes(0);
    expect(result.client).toMatchObject({ id: "A" });

    // Semantically different config should recreate the client.
    setConfig({ appId: "B" });
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
});

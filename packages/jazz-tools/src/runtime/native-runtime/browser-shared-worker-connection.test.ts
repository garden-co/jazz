import { describe, expect, it } from "vitest";
import { createBrowserSharedWorkerBaseName } from "./browser-shared-worker-connection.js";

describe("browser SharedWorker realm identity", () => {
  it("keeps foreground leases and runtime admission in one physical realm", () => {
    const dbName = "physical-root";

    // A foreground lease is acquired before the complete runtime config is
    // assembled. Its worker identity must therefore depend only on the
    // physical root and worker assets, not on a separately supplied auth
    // scope. The IndexedDB owner marker performs that admission inside this
    // single realm.
    const leaseWorker = createBrowserSharedWorkerBaseName(undefined, dbName);
    const runtimeWorker = createBrowserSharedWorkerBaseName(undefined, dbName);

    expect(leaseWorker).toBe(runtimeWorker);
    expect(leaseWorker).toContain(dbName);
    expect(leaseWorker).not.toContain("authSessionKey");
  });

  it("keeps incompatible worker assets in separate realms", () => {
    const dbName = "physical-root";
    const current = createBrowserSharedWorkerBaseName(
      { wasmUrl: "https://assets.test/current.wasm", wasmVersion: "current" },
      dbName,
    );
    const next = createBrowserSharedWorkerBaseName(
      { wasmUrl: "https://assets.test/next.wasm", wasmVersion: "next" },
      dbName,
    );

    expect(current).not.toBe(next);
  });
});

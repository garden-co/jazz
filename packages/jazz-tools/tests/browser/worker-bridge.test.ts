/// <reference types="vite/client" />

/**
 * Browser integration tests for the SharedWorker + IndexedDB runtime.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real jazz-wasm, a real SharedWorker, and real IndexedDB storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 *
 * Part 1 of the bridge suite: foreground leases, worker lifecycle, CRUD and
 * storage reset. worker-bridge-sync.test.ts and worker-bridge-tabs.test.ts hold
 * the rest; shared fixtures live in worker-bridge-harness.ts.
 */

import { describe, it, expect, vi } from "vitest";
import {
  createBrowserTestDb as createDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";
import {
  Db,
  getDbSubscriptionSource,
  resolveDefaultPersistentDbName,
} from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import {
  INDEXEDDB_STORAGE_MANIFEST,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";
import {
  createBrowserSharedWorkerBaseName,
  SharedBrowserForegroundNodeLease,
} from "../../src/runtime/native-runtime/browser-shared-worker-connection.js";
import { NativeRuntimeAdapter } from "../../src/runtime/native-runtime/native-runtime-adapter.js";
import { setBrowserFollowerProbeTimingForTest } from "../../src/runtime/native-runtime/browser-follower-connection.js";
import { createOpenTransactionId } from "../../src/runtime/client.js";
import { loadWasmModule } from "../../src/runtime/wasm-loader.js";
import { createBrowserStorageOwner } from "../../src/runtime/browser-worker-config.js";
import { getJazzServerJwtForUser } from "./testing-server.js";
import {
  createRemoteBrowserDb,
  deleteRemoteBrowserIndexedDbAndWaitForReload,
  insertRemoteBrowserDbRow,
  queryRemoteBrowserDbRows,
  restartRemoteBrowserDb,
} from "./remote-browser-db.js";
import {
  BrowserWorkerUnresponsiveError,
  serializeBrowserRelayError,
  type BrowserForegroundNodeLeaseAcquireRequest,
  type BrowserForegroundNodeLeasePortRequest,
  type BrowserForegroundNodeLeaseProbeRequest,
  deserializeBrowserRelayError,
  type BrowserInspectorControlRequest,
  type BrowserRelayError,
} from "../../src/runtime/native-runtime/browser-worker-protocol.js";
import {
  workerFaultBundleUrl,
  listWorkerLifecycle,
  LIVENESS_TEST_PROBE_TIMING,
  LIVENESS_TEST_SIGNAL_MS,
  app,
  todos,
  Todo,
  allTodos,
  catalogueTodos,
  startRawForegroundLease,
  waitForCatalogueTodos,
  publishCatalogueSchemaFamily,
  publishSyncServerSchemaAndPermissions,
  replaceStorageManifest,
  rawStorageRecords,
  requestResult,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

declare const __JAZZ_BROWSER_SOAK__: string;

describe("foreground lease terminal policy with real IndexedDB and WASM", () => {
  async function storeBackedLease() {
    const dbName = uniqueDbName("terminal-lease-store");
    const store = await IndexedDbPageStore.open(dbName);
    const channel = new MessageChannel();
    let runtime: NativeRuntimeAdapter | undefined;
    try {
      const allocation = await store.acquireForegroundNodeLease();
      const wasm = await loadWasmModule();
      runtime = new NativeRuntimeAdapter(
        wasm.WasmDb,
        app.wasmSchema,
        allocation.node,
        new TextEncoder().encode('["urn:jazz:test","terminal-lease"]'),
        1,
        true,
        { backendMode: true },
      );
      runtime.seedForegroundTxTimeHighWater(allocation.confirmedTxTime);
      let committed!: () => void;
      let failed!: (error: unknown) => void;
      const terminalCommitted = new Promise<void>((resolve, reject) => {
        committed = resolve;
        failed = reject;
      });
      void terminalCommitted.catch(() => undefined);
      const terminalOperations: Promise<void>[] = [];
      channel.port2.onmessage = (
        event: MessageEvent<
          | BrowserForegroundNodeLeaseProbeRequest
          | BrowserForegroundNodeLeaseAcquireRequest
          | BrowserForegroundNodeLeasePortRequest
        >,
      ) => {
        const message = event.data;
        if (message.type === "probe-foreground-node-lease-worker") {
          channel.port2.postMessage({
            type: "foreground-node-lease-worker-alive",
            attemptId: message.attemptId,
          });
        } else if (message.type === "acquire-foreground-node-lease") {
          channel.port2.postMessage({
            type: "foreground-node-lease-ready",
            ...allocation,
            confirmedTxTime: allocation.confirmedTxTime.toString(),
          });
        } else {
          // Only transport delivery is controlled. A successful result cannot
          // be released until the real IndexedDB transaction has committed.
          const operation =
            message.type === "return-foreground-node-lease"
              ? store.returnForegroundNodeLease(allocation.leaseId, BigInt(message.confirmedTxTime))
              : store.retireForegroundNodeLease(allocation.leaseId);
          terminalOperations.push(operation);
          void operation.then(committed, (error) => {
            failed(error);
            channel.port2.postMessage({
              type: "foreground-node-lease-result",
              error: serializeBrowserRelayError(error),
            });
          });
        }
      };
      const lease = await SharedBrowserForegroundNodeLease.acquireFromPort(channel.port1, {
        dbName,
        storageOwner: "terminal-lease-control",
      });
      const post = vi.spyOn(channel.port1, "postMessage");
      const close = vi.spyOn(channel.port1, "close");
      return {
        dbName,
        store,
        lease,
        runtime,
        post,
        close,
        terminalCommitted,
        acknowledge() {
          channel.port2.postMessage({ type: "foreground-node-lease-result" });
        },
        async dispose() {
          channel.port1.close();
          channel.port2.close();
          await Promise.allSettled(terminalOperations);
          await runtime!.close();
          await store.close();
          await IndexedDbPageStore.destroy(dbName);
        },
      };
    } catch (error) {
      channel.port1.close();
      channel.port2.close();
      await runtime?.close();
      await store.close();
      await IndexedDbPageStore.destroy(dbName);
      throw error;
    }
  }

  it("retains a committed quiesced return and its final durable HWM after local abandonment", async () => {
    const fixture = await storeBackedLease();
    const { runtime, lease } = fixture;
    let releaseSource!: () => void;
    const sourceGate = new Promise<void>((resolve) => {
      releaseSource = resolve;
    });
    let sourceStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      sourceStarted = resolve;
    });
    let write: Promise<unknown> | undefined;
    try {
      const initialHighWater = runtime.foregroundTxTimeHighWater();
      const oldBatch = createOpenTransactionId();
      runtime.beginTransaction("mergeable", oldBatch);
      write = runtime.streamingMutation(
        "insert",
        "projects",
        {},
        "name",
        (async function* () {
          sourceStarted();
          await sourceGate;
          yield "write admitted before handoff";
        })(),
      );
      await withTimeout(
        Promise.race([started, write]),
        5_000,
        "real native streaming write did not start",
      );
      let captured = false;
      const handoff = runtime.quiesceForegroundTxTimeHighWater().then((value) => {
        captured = true;
        return value;
      });
      await Promise.resolve();
      expect(captured).toBe(false);
      expect(fixture.post).not.toHaveBeenCalled();
      releaseSource();
      await write;
      const highWater = await handoff;
      expect(highWater).toBeGreaterThan(initialHighWater);
      expect(() => runtime.commitTransaction(oldBatch)).toThrow("native runtime is closed");
      expect(() => runtime.beginTransaction("mergeable", createOpenTransactionId())).toThrow(
        "native runtime is closed",
      );
      expect(() =>
        runtime.insert("projects", { name: { type: "Text", value: "too late" } }),
      ).toThrow("native runtime is closed");

      const failure = new BrowserWorkerUnresponsiveError("worker reply was lost after commit");
      const returned = lease.returnWithHighWater(highWater);
      const rejected = expect(returned).rejects.toBe(failure);
      await withTimeout(fixture.terminalCommitted, 5_000, "real return transaction did not commit");
      expect(await fixture.store.foregroundNodeLeaseNodeState(lease.node)).toBe("reusable");
      // Reuse may happen before the old page detects failure. An old-page
      // abandonment cannot revoke this successor or lower its final floor.
      await fixture.store.close();
      const reopened = await IndexedDbPageStore.open(fixture.dbName);
      try {
        const successor = await reopened.acquireForegroundNodeLease();
        expect(successor.node).toEqual(lease.node);
        expect(successor.confirmedTxTime).toBe(highWater);
        lease.abandonAfterWorkerFailure(failure);
        await rejected;
        expect(fixture.post).toHaveBeenCalledExactlyOnceWith({
          type: "return-foreground-node-lease",
          confirmedTxTime: highWater.toString(),
        });
        expect(await reopened.foregroundNodeLeaseNodeState(successor.node)).toBe("active");
        expect(fixture.close).not.toHaveBeenCalled();
        fixture.acknowledge();
        await vi.waitFor(() => expect(fixture.close).toHaveBeenCalledOnce());
        await expect(lease.returnWithHighWater(highWater + 1n)).rejects.toBe(failure);
        await expect(lease.retire()).rejects.toBe(failure);
        expect(runtime.foregroundTxTimeHighWater()).toBe(highWater);
        await reopened.returnForegroundNodeLease(successor.leaseId, highWater);
      } finally {
        await reopened.close();
      }
    } finally {
      releaseSource();
      await write?.catch(() => undefined);
      await fixture.dispose();
    }
  }, 15_000);

  it("durably retires a quiesced foreground abandoned before any terminal request", async () => {
    const fixture = await storeBackedLease();
    try {
      await fixture.runtime.quiesceForegroundTxTimeHighWater();
      const failure = new BrowserWorkerUnresponsiveError("worker stopped before lease finish");
      fixture.lease.abandonAfterWorkerFailure(failure);
      await expect(fixture.lease.retire()).rejects.toBe(failure);
      await withTimeout(
        fixture.terminalCommitted,
        5_000,
        "real retirement transaction did not commit",
      );
      expect(fixture.post).toHaveBeenCalledExactlyOnceWith({
        type: "retire-foreground-node-lease",
      });
      expect(await fixture.store.foregroundNodeLeaseNodeState(fixture.lease.node)).toBe("retired");
      await fixture.store.close();
      const reopened = await IndexedDbPageStore.open(fixture.dbName);
      try {
        const successor = await reopened.acquireForegroundNodeLease();
        expect(successor.node).not.toEqual(fixture.lease.node);
        expect(successor.confirmedTxTime).toBe(0n);
        expect(() =>
          fixture.runtime.insert("projects", { name: { type: "Text", value: "too late" } }),
        ).toThrow("native runtime is closed");
        expect(fixture.close).not.toHaveBeenCalled();
        fixture.acknowledge();
        await vi.waitFor(() => expect(fixture.close).toHaveBeenCalledOnce());
        await expect(fixture.lease.returnWithHighWater(0n)).rejects.toBe(failure);
        await reopened.retireForegroundNodeLease(successor.leaseId);
      } finally {
        await reopened.close();
      }
    } finally {
      await fixture.dispose();
    }
  }, 15_000);
});

describe("SharedWorker bridge with IndexedDB", () => {
  const {
    errorListeners,
    trackRemoteBrowserDb,
    track,
    trackSubscription,
    untrack,
    shutdownDbAndWorker,
  } = useSharedWorkerBridgeHarness();

  it("retains a queued first-owner allocation after the preceding lease returns", async () => {
    const dbName = uniqueDbName("queued-first-owner");
    const storageOwner = createBrowserStorageOwner({
      appId: uniqueDbName("queued-first-owner-app"),
      secret: generateAuthSecret(),
    });
    const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
    const createPort = () => {
      const worker = new SharedWorker(new URL("./jazz-broker-worker-test.ts", import.meta.url), {
        type: "module",
        name: `${workerName}:generation-0`,
      });
      return worker.port;
    };
    const first = startRawForegroundLease(createPort(), { dbName, storageOwner });
    const second = startRawForegroundLease(createPort(), {
      dbName,
      storageOwner,
      testDelayBeforeLeaseAllocationMs: 250,
    });

    const [firstLease] = await withTimeout(
      Promise.all([first.ready, second.queued]),
      5_000,
      "second foreground allocation did not enter the admitted queue",
    );
    await firstLease.returnWithHighWater(11n);
    const secondLease = await withTimeout(
      second.ready,
      5_000,
      "returning the first lease released a physical owner with admitted allocation work",
    );
    // The queued request begins only after the clean return commits, so it is
    // allowed—and expected—to reuse that safely handed-off identity.
    expect(secondLease.node).toEqual(firstLease.node);
    await secondLease.returnWithHighWater(22n);

    // Both balanced reservations are now gone. A successor realm can claim
    // the physical root and reuse the final clean handoff.
    await sleep(100);
    const successorWorker = new SharedWorker(
      new URL("./jazz-broker-worker-test.ts", import.meta.url),
      { type: "module", name: `${workerName}:generation-1` },
    );
    const successor = await withTimeout(
      startRawForegroundLease(successorWorker.port, { dbName, storageOwner }).ready,
      5_000,
      "balanced queued allocations left the physical root unavailable to a successor realm",
    );
    expect(successor.node).toEqual(secondLease.node);
    await successor.returnWithHighWater(33n);
  }, 10_000);

  it("releases a terminally failed pending allocation for a clean successor", async () => {
    const dbName = uniqueDbName("failed-pending-owner");
    const storageOwner = createBrowserStorageOwner({
      appId: uniqueDbName("failed-pending-owner-app"),
      secret: generateAuthSecret(),
    });
    const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
    const failedWorker = new SharedWorker(
      new URL("./jazz-broker-worker-test.ts", import.meta.url),
      { type: "module", name: `${workerName}:generation-0` },
    );
    const failed = startRawForegroundLease(failedWorker.port, {
      dbName,
      storageOwner,
      testDelayBeforeLeaseAllocationMs: 1_001,
    });
    await expect(failed.ready).rejects.toThrow("Invalid foreground lease test delay");

    await sleep(100);
    const successorWorker = new SharedWorker(
      new URL("./jazz-broker-worker-test.ts", import.meta.url),
      { type: "module", name: `${workerName}:generation-1` },
    );
    const successor = await withTimeout(
      startRawForegroundLease(successorWorker.port, { dbName, storageOwner }).ready,
      5_000,
      "failed pending allocation retained the physical owner",
    );
    await successor.returnWithHighWater(44n);
  }, 10_000);

  it("coalesces concurrent first-tab durable-owner admission in one worker realm", async () => {
    const dbName = uniqueDbName("concurrent-first-owner");
    const storageOwner = createBrowserStorageOwner({
      appId: uniqueDbName("concurrent-first-owner-app"),
      secret: generateAuthSecret(),
    });

    const [first, second] = await withTimeout(
      Promise.all([
        SharedBrowserForegroundNodeLease.acquire({ dbName, storageOwner }),
        SharedBrowserForegroundNodeLease.acquire({ dbName, storageOwner }),
      ]),
      5_000,
      "concurrent first tabs did not share durable physical-owner admission",
    );
    try {
      expect(second.node).not.toEqual(first.node);
    } finally {
      // A second first-open transaction must not have retired the first live
      // identity as "abandoned". Clean return rejects an unknown/retired
      // lease, so both succeeding proves both remained durably active.
      await Promise.all([first.returnWithHighWater(11n), second.returnWithHighWater(22n)]);
    }
  }, 10_000);

  /**
   * A foreground which times out while the worker is still delivering its
   * durable identity must cancel/retire that lease before a later foreground
   * opens the same root.
   *
   * first tab ──acquire──► worker ──durably allocate──► delayed delivery
   * first tab ──cancel───► worker ──retire────────────► durable lease pool
   * second tab ──acquire──► worker ──fresh node──► second tab
   */
  it("retires a foreground lease when cancellation races durable allocation delivery", async () => {
    const dbName = uniqueDbName("cancelled-foreground-lease");
    const storageOwner = createBrowserStorageOwner({
      appId: uniqueDbName("cancelled-foreground-lease-app"),
      secret: generateAuthSecret(),
    });
    const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
    const worker = new SharedWorker(new URL("./jazz-broker-worker-test.ts", import.meta.url), {
      type: "module",
      name: `${workerName}:generation-0`,
    });
    const port = worker.port;
    port.start();
    let unexpectedlyIssued = false;
    let allocatedNode: Uint8Array | null = null;
    let cancellationLeaseState: string | undefined;
    try {
      await withTimeout(
        new Promise<void>((resolve, reject) => {
          const onMessage = (
            event: MessageEvent<{
              type?: string;
              node?: Uint8Array;
              error?: BrowserRelayError;
              testLeaseState?: string;
            }>,
          ) => {
            if (event.data?.type === "foreground-node-lease-ready" && event.data.node) {
              unexpectedlyIssued = true;
            }
            if (event.data?.type === "foreground-node-lease-test-allocated" && event.data.node) {
              allocatedNode = event.data.node.slice();
              // Follow the durable allocation receipt rather than a wall-clock
              // guess: sealed CI can otherwise cancel during root admission,
              // before a lease exists to retire.
              port.postMessage({ type: "cancel-foreground-node-lease" });
            }
            if (event.data?.type === "foreground-node-lease-error" && event.data.error) {
              port.removeEventListener("message", onMessage);
              reject(deserializeBrowserRelayError(event.data.error));
            }
            if (event.data?.type === "foreground-node-lease-cancelled") {
              cancellationLeaseState = event.data.testLeaseState;
              port.removeEventListener("message", onMessage);
              resolve();
            }
          };
          port.addEventListener("message", onMessage);
          port.postMessage({
            type: "acquire-foreground-node-lease",
            dbName,
            storageOwner,
            testDelayAfterLeaseAllocationMs: 250,
          });
        }),
        5_000,
        "in-flight foreground lease cancellation was not acknowledged after cleanup",
      );
    } finally {
      port.close();
    }
    expect(unexpectedlyIssued).toBe(false);
    expect(allocatedNode).not.toBeNull();
    expect(cancellationLeaseState).toBe("retired");

    // Cancellation releases the now-idle physical realm. Let the browser
    // finish that close before opening its successor generation.
    await sleep(100);

    // A cancelled-but-issued identity is retired, never put back into the
    // reusable pool. A later foreground must receive a distinct node.
    const successor = await withTimeout(
      SharedBrowserForegroundNodeLease.acquire({ dbName, storageOwner }),
      5_000,
      "foreground lease cancellation left the physical root unavailable",
    );
    try {
      expect(successor.node).not.toEqual(allocatedNode);
    } finally {
      await successor.retire();
    }
  }, 15_000);

  it("does not expose the foreground lease test seam to an ordinary worker client", async () => {
    const dbName = uniqueDbName("ordinary-foreground-lease");
    const storageOwner = createBrowserStorageOwner({
      appId: uniqueDbName("ordinary-foreground-lease-app"),
      secret: generateAuthSecret(),
    });
    const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
    const worker = new SharedWorker(
      new URL("../../src/worker/jazz-broker-worker.ts", import.meta.url),
      { type: "module", name: `${workerName}:generation-0` },
    );
    const port = worker.port;
    port.start();
    let sawTestAllocation = false;
    try {
      await withTimeout(
        new Promise<void>((resolve, reject) => {
          const onMessage = (event: MessageEvent<{ type?: string; error?: BrowserRelayError }>) => {
            if (event.data?.type === "foreground-node-lease-test-allocated") {
              sawTestAllocation = true;
            }
            if (event.data?.type === "foreground-node-lease-error" && event.data.error) {
              port.removeEventListener("message", onMessage);
              reject(deserializeBrowserRelayError(event.data.error));
            }
            if (event.data?.type === "foreground-node-lease-ready") {
              port.postMessage({ type: "retire-foreground-node-lease" });
            }
            if (event.data?.type === "foreground-node-lease-result") {
              port.removeEventListener("message", onMessage);
              resolve();
            }
          };
          port.addEventListener("message", onMessage);
          // The production worker has no hook installation, so this
          // test-only scheduling field is inert even when a raw client sends it.
          port.postMessage({
            type: "acquire-foreground-node-lease",
            dbName,
            storageOwner,
            testDelayAfterLeaseAllocationMs: 1_000,
          });
        }),
        5_000,
        "ordinary foreground lease client did not finish",
      );
    } finally {
      port.close();
    }
    expect(sawTestAllocation).toBe(false);
  }, 10_000);

  it("shares one stable test-worker realm between foreground lease clients", async () => {
    const dbName = uniqueDbName("shared-test-worker-lease");
    const storageOwner = createBrowserStorageOwner({
      appId: uniqueDbName("shared-test-worker-lease-app"),
      secret: generateAuthSecret(),
    });
    const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
    const workerUrl = new URL("./jazz-broker-worker-test.ts", import.meta.url);
    const acquire = async () => {
      const worker = new SharedWorker(workerUrl, {
        type: "module",
        name: `${workerName}:generation-0`,
      });
      const port = worker.port;
      port.start();
      let allocation: { node: Uint8Array; workerRealmId: string } | null = null;
      await withTimeout(
        new Promise<void>((resolve, reject) => {
          const onMessage = (
            event: MessageEvent<{
              type?: string;
              error?: BrowserRelayError;
              node?: Uint8Array;
              workerRealmId?: string;
            }>,
          ) => {
            if (
              event.data?.type === "foreground-node-lease-test-allocated" &&
              event.data.node &&
              event.data.workerRealmId
            ) {
              allocation = {
                node: event.data.node.slice(),
                workerRealmId: event.data.workerRealmId,
              };
            }
            if (event.data?.type === "foreground-node-lease-error" && event.data.error) {
              port.removeEventListener("message", onMessage);
              reject(deserializeBrowserRelayError(event.data.error));
            }
            if (event.data?.type === "foreground-node-lease-ready") {
              port.removeEventListener("message", onMessage);
              resolve();
            }
          };
          port.addEventListener("message", onMessage);
          port.postMessage({
            type: "acquire-foreground-node-lease",
            dbName,
            storageOwner,
            testDelayAfterLeaseAllocationMs: 0,
          });
        }),
        5_000,
        "test foreground lease client did not receive a lease",
      );
      if (!allocation) throw new Error("test foreground lease allocation was not observed");
      return {
        ...allocation,
        async retire() {
          await withTimeout(
            new Promise<void>((resolve, reject) => {
              const onMessage = (
                event: MessageEvent<{ type?: string; error?: BrowserRelayError }>,
              ) => {
                if (event.data?.type !== "foreground-node-lease-result") return;
                port.removeEventListener("message", onMessage);
                if (event.data.error) reject(deserializeBrowserRelayError(event.data.error));
                else resolve();
              };
              port.addEventListener("message", onMessage);
              port.postMessage({ type: "retire-foreground-node-lease" });
            }),
            5_000,
            "test foreground lease did not retire",
          );
          port.close();
        },
      };
    };

    const first = await acquire();
    let second: Awaited<ReturnType<typeof acquire>> | null = null;
    try {
      second = await acquire();
      expect(first.workerRealmId).toBe(second.workerRealmId);
      expect(first.node).not.toEqual(second.node);
    } finally {
      await Promise.all([first.retire(), second?.retire()]);
    }
  }, 15_000);

  it("fences a generation-advanced worker realm until its live predecessor releases the physical root", async () => {
    const appId = uniqueDbName("physical-worker-epoch-app");
    const dbName = uniqueDbName("physical-worker-epoch-root");
    const secret = generateAuthSecret();
    const config = { appId, secret, driver: { type: "persistent" as const, dbName } };
    // `driver.dbName` is the caller-selected logical base. The worker and
    // IndexedDB liveness fence deliberately protect its auth-scoped physical
    // root, which is the name `createDb` actually opens.
    const first = track(await createDb(config));
    try {
      // `createDb` turns a local-first secret into its canonical session
      // before deriving the physical root. Derive from that resolved config,
      // rather than from the caller input whose secret has not yet become a
      // session identity.
      const physicalDbName = resolveDefaultPersistentDbName(first.config);
      // Materialize both the foreground lease and worker runtime before
      // deliberately advancing the page-side generation key.
      await first.all(allTodos, { tier: "local" });
      const workerName = createBrowserSharedWorkerBaseName(undefined, physicalDbName);
      localStorage.setItem(`jazz:shared-worker-generation:${workerName}`, "1");

      // Planted overlap: generation one names a distinct SharedWorker even
      // though generation zero is live. It must fail before it can recover
      // generation zero's foreground lease pool.
      await expect(createDb(config)).rejects.toThrow("active in another Jazz SharedWorker realm");

      await first.shutdown();
      untrack(first);
      await sleep(100);

      const successor = track(await createDb(config));
      try {
        await expect(successor.all(allTodos, { tier: "local" })).resolves.toEqual([]);
      } finally {
        await successor.shutdown();
        untrack(successor);
      }
    } finally {
      await first.shutdown().catch(() => undefined);
      untrack(first);
    }
  });

  it("releases an invalidated physical-worker epoch so a successor generation can reopen", async () => {
    const appId = uniqueDbName("invalidated-physical-worker-epoch-app");
    const dbName = uniqueDbName("invalidated-physical-worker-epoch-root");
    const secret = generateAuthSecret();
    const storageOwner = createBrowserStorageOwner({ appId, secret });
    const first = await SharedBrowserForegroundNodeLease.acquire({ dbName, storageOwner });
    try {
      const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
      localStorage.setItem(`jazz:shared-worker-generation:${workerName}`, "1");
      await expect(
        SharedBrowserForegroundNodeLease.acquire({ dbName, storageOwner }),
      ).rejects.toThrow("active in another Jazz SharedWorker realm");

      // Planted lifecycle transition: this is not a clean worker handoff.
      // IDB versionchange/delete invalidates the live worker handle, so the
      // successor must be admitted after that handle releases its Web Lock.
      await withTimeout(
        requestResult(indexedDB.deleteDatabase(dbName)),
        5_000,
        "External IndexedDB invalidation remained blocked by the lease-only worker handle",
      );
      await sleep(100);

      const successor = await withTimeout(
        SharedBrowserForegroundNodeLease.acquire({ dbName, storageOwner }),
        5_000,
        "Successor generation did not acquire the invalidated physical root",
      );
      try {
        expect(successor.node).not.toEqual(first.node);
      } finally {
        await successor.retire();
      }
    } finally {
      await withTimeout(
        first.retire(),
        1_000,
        "Invalidated predecessor lease did not settle during test cleanup",
      ).catch(() => undefined);
    }
  }, 15_000);

  // -------------------------------------------------------------------------
  // 1. Worker initialization
  // -------------------------------------------------------------------------

  it("creates Db with worker in browser environment", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent" },
      }),
    );
    expect(db).toBeDefined();
    expect(db).toBeInstanceOf(Db);
  });

  it("keeps public shutdown alive with pongs then rejects after silent worker death", async () => {
    const capability = uniqueDbName("follower-fault");
    setBrowserFollowerProbeTimingForTest(LIVENESS_TEST_PROBE_TIMING);
    const workerUrl = new URL(await workerFaultBundleUrl(), globalThis.location.href);
    workerUrl.searchParams.set("followerFault", capability);
    const control = new BroadcastChannel(capability);
    const receive = (type: string) =>
      new Promise<void>((resolve) => {
        const listener = (event: MessageEvent<{ type: string }>) => {
          if (event.data.type !== type) return;
          control.removeEventListener("message", listener);
          resolve();
        };
        control.addEventListener("message", listener);
      });
    let db: Db | undefined;
    try {
      db = track(
        await createDb({
          appId: "follower-silent-death",
          driver: { type: "persistent", dbName: uniqueDbName("follower-silent-death") },
          schema: app,
          runtimeSources: {
            brokerWorkerUrl: workerUrl.href,
            wasmVersion: "follower-liveness-test",
          },
        }),
      );
      await db.all(allTodos, { tier: "local" });
      const armed = receive("holding-close");
      control.postMessage({ type: "hold-close" });
      await withTimeout(armed, 5_000, "test worker did not arm the pending control hold");
      const held = receive("close-held");
      let outcome: "pending" | "resolved" | "rejected" = "pending";
      const shutdown = db.shutdown().then(
        () => {
          outcome = "resolved";
          return undefined;
        },
        (error: unknown) => {
          outcome = "rejected";
          return error;
        },
      );
      // Ordinary shutdown returns its foreground lease before sending close.
      // Killing here leaves no unrelated lease-return RPC in test cleanup.
      await withTimeout(held, 5_000, "public shutdown did not reach the real worker close");
      // Three real broker replies span more than the entire silent-death
      // bound. Neither the public operation nor its failure is fabricated.
      for (let index = 0; index < 3; index++) {
        await withTimeout(
          receive("pong-sent"),
          LIVENESS_TEST_SIGNAL_MS,
          "real worker did not answer its liveness probe",
        );
        expect(outcome).toBe("pending");
      }
      control.postMessage({ type: "die" });
      const error = await withTimeout(
        shutdown,
        LIVENESS_TEST_SIGNAL_MS,
        "silent worker death left public shutdown pending",
      );
      expect(outcome).toBe("rejected");
      if (!(error instanceof Error)) throw new Error("Expected public shutdown to reject");
      expect(error.message).toMatch(/outcomes are unknown.*not retried/);
    } finally {
      // Preserve setup/assertion failures without leaving this deliberately
      // killed fixture in the shared afterEach's unbounded shutdown loop.
      if (db) {
        untrack(db);
        void db.shutdown().catch(() => undefined);
      }
      control.postMessage({ type: "die" });
      control.close();
      setBrowserFollowerProbeTimingForTest();
    }
  }, 60_000);

  it("settles public shutdown after a pending follower operation rejects on silent worker death", async () => {
    const capability = uniqueDbName("follower-death-cleanup");
    setBrowserFollowerProbeTimingForTest(LIVENESS_TEST_PROBE_TIMING);
    const workerUrl = new URL(await workerFaultBundleUrl(), globalThis.location.href);
    workerUrl.searchParams.set("followerFault", capability);
    const control = new BroadcastChannel(capability);
    const receive = (type: string) =>
      new Promise<void>((resolve) => {
        const listener = (event: MessageEvent<{ type: string }>) => {
          if (event.data.type !== type) return;
          control.removeEventListener("message", listener);
          resolve();
        };
        control.addEventListener("message", listener);
      });
    let db: Db | undefined;
    try {
      db = track(
        await createDb({
          appId: "follower-death-cleanup",
          driver: { type: "persistent", dbName: uniqueDbName("follower-death-cleanup") },
          schema: app,
          runtimeSources: {
            brokerWorkerUrl: workerUrl.href,
            wasmVersion: "follower-liveness-test",
          },
        }),
      );
      await db.all(allTodos, { tier: "local" });
      const armed = receive("holding-pending-writes");
      control.postMessage({ type: "hold-pending-writes" });
      await withTimeout(armed, 5_000, "test worker did not arm the pending control hold");
      const held = receive("pending-writes-held");
      const pending = db.shutdown({ waitForSync: true }).then(
        () => undefined,
        (error: unknown) => error,
      );
      await withTimeout(held, 5_000, "public graceful shutdown did not reach the worker barrier");
      control.postMessage({ type: "die" });
      const error = await withTimeout(
        pending,
        LIVENESS_TEST_SIGNAL_MS,
        "silent worker death left the follower operation pending",
      );
      if (!(error instanceof Error) || !(error.cause instanceof Error)) {
        throw new Error("Expected graceful shutdown to retain the follower failure cause");
      }
      expect(error.cause.message).toMatch(/outcomes are unknown.*not retried/);

      // A terminal local cleanup failure is honest; an unanswered companion
      // lease-return RPC must not keep the public Db alive indefinitely.
      // Both observers precede the guard so rejection is not confused with a
      // harness timeout and does not become an unhandled rejection.
      const cleanup = db.shutdown().catch(() => undefined);
      await withTimeout(
        cleanup,
        5_000,
        "Db.shutdown remained pending after the follower connection had already failed",
      );
    } finally {
      // Setup failures and the bounded cleanup assertion above remain the
      // failure signal; never repeat a hanging shutdown in shared afterEach.
      if (db) {
        untrack(db);
        void db.shutdown().catch(() => undefined);
      }
      control.postMessage({ type: "die" });
      control.close();
      setBrowserFollowerProbeTimingForTest();
    }
  }, 45_000);

  it("exposes a bounded redacted worker lifecycle ledger to the owning inspector", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("worker-lifecycle-ledger");
    const db = track(
      await createDb({
        appId: syncServer.appId,
        serverUrl: syncServer.serverUrl,
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: uniqueDbName("worker-lifecycle-ledger") },
        logLevel: "trace",
        schema: app,
      }),
    );
    // `createDb` resolves after the foreground runtime is available; the
    // worker follower is installed on the first public read.
    await db.all(allTodos, { tier: "local" });
    const inspector = await db.openInspectorControlPort();
    inspector.start();
    try {
      const entries = await listWorkerLifecycle(inspector);
      expect(entries.map((entry) => entry.event)).toEqual(
        expect.arrayContaining(["bootstrap-start", "peer-attached"]),
      );
      expect(entries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            sequence: expect.any(Number),
            peerCount: expect.any(Number),
            pendingBootstraps: expect.any(Number),
            activeLeases: expect.any(Number),
          }),
        ]),
      );
    } finally {
      inspector.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
    }
  });

  it("registers concurrent local subscriptions before worker admission while withholding openings", async () => {
    const db = track(
      await createDb({
        appId: "concurrent-local-subscription-admission",
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: uniqueDbName("concurrent-local-subscription") },
      }),
    );
    // Selecting the schema begins the worker handshake but cannot complete it
    // in this same call stack. The registration spy distinguishes the required
    // native ordering from the old workaround that waited before subscribing.
    const client = (
      db as unknown as {
        getClient(schema: typeof todos._schema): {
          subscribeInternal: (...args: never[]) => number;
        };
      }
    ).getClient(todos._schema);
    const nativeSubscribe = vi.spyOn(client, "subscribeInternal");
    const source = getDbSubscriptionSource(db);
    const firstDeltas: unknown[] = [];
    const secondDeltas: unknown[] = [];
    const first = source.subscribeDelta(todos, (delta) => firstDeltas.push(delta), {
      tier: "local",
    });
    const second = source.subscribeDelta(todos, (delta) => secondDeltas.push(delta), {
      tier: "local",
    });
    try {
      expect(first.ready).toBeDefined();
      expect(second.ready).toBeDefined();
      expect(nativeSubscribe).toHaveBeenCalledTimes(2);
      expect(firstDeltas).toEqual([]);
      expect(secondDeltas).toEqual([]);
      await expect(Promise.all([first.ready, second.ready])).resolves.toEqual([
        undefined,
        undefined,
      ]);
    } finally {
      first();
      second();
    }
  });

  it("rejects createDb operation-scoped when its foreground lease cannot open durable storage", async () => {
    const ambientErrors: string[] = [];
    const unhandledRejections: string[] = [];
    const recordAmbientError = (event: ErrorEvent) => {
      ambientErrors.push(event.error instanceof Error ? event.error.message : event.message);
    };
    const recordUnhandledRejection = (event: PromiseRejectionEvent) => {
      event.preventDefault();
      unhandledRejections.push(
        event.reason instanceof Error ? event.reason.message : String(event.reason),
      );
    };
    globalThis.addEventListener("error", recordAmbientError);
    globalThis.addEventListener("unhandledrejection", recordUnhandledRejection);
    errorListeners.add(recordAmbientError);
    const dbName = uniqueDbName("corrupt-storage-open");
    const secret = generateAuthSecret();
    const config = { appId: "test-app", secret, driver: { type: "persistent" as const, dbName } };
    try {
      const initial = track(await createDb(config));
      await initial
        .insert(todos, { title: "durable sentinel", done: false })
        .wait({ tier: "local" });
      await initial.shutdown();
      untrack(initial);

      // Local-first caller credentials are normalized to a canonical session
      // during `createDb`, so the actual physical root must be derived from
      // the resolved Db config rather than the pre-normalization input.
      const physicalDbName = resolveDefaultPersistentDbName(initial.config);

      await replaceStorageManifest(physicalDbName, {
        ...INDEXEDDB_STORAGE_MANIFEST,
        storageEpoch: 99,
      });
      const recordsBeforeRead = await rawStorageRecords(physicalDbName);

      // Persistent create must acquire a durable foreground-node lease before
      // any synchronous mutation can mint a transaction identity. Storage
      // readiness therefore belongs to createDb, while schema selection stays
      // lazy. The original structured worker error must reject that operation
      // directly instead of collapsing to a message-only main-thread Error.
      let openFailure: unknown;
      try {
        await createDb(config);
      } catch (error) {
        openFailure = error;
      }
      expect(openFailure).toBeInstanceOf(Error);
      if (!(openFailure instanceof Error)) throw new Error("Expected browser worker open to fail");
      expect(openFailure.cause).toBeUndefined();
      await sleep(0);
      expect(ambientErrors).toEqual([]);
      expect(unhandledRejections).toEqual([]);
      expect(await rawStorageRecords(physicalDbName)).toEqual(recordsBeforeRead);
    } finally {
      globalThis.removeEventListener("error", recordAmbientError);
      globalThis.removeEventListener("unhandledrejection", recordUnhandledRejection);
      errorListeners.delete(recordAmbientError);
    }
  });

  // -------------------------------------------------------------------------
  // 2. Insert + local query through worker bridge
  // -------------------------------------------------------------------------

  it("inserts a row and queries it back", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("insert-query") },
      }),
    );

    // Insert (sync — runs on main-thread in-memory runtime)
    const {
      value: { id },
    } = db.insert(todos, { title: "Buy milk", done: false });
    expect(id).toBeTruthy();
    expect(typeof id).toBe("string");

    // Query (async — runs on main-thread runtime)
    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].id).toBe(id);
    expect(results[0].title).toBe("Buy milk");
    expect(results[0].done).toBe(false);
  });

  it("inserts multiple rows and queries all", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("multi-insert") },
      }),
    );

    db.insert(todos, { title: "Task A", done: false });
    db.insert(todos, { title: "Task B", done: true });
    db.insert(todos, { title: "Task C", done: false });

    const results = await db.all(allTodos);
    expect(results.length).toBe(3);

    const titles = results.map((r) => r.title).sort();
    expect(titles).toEqual(["Task A", "Task B", "Task C"]);
  });

  it("sync insert before bridge init is persisted after init completes", async () => {
    const dbName = uniqueDbName("sync-insert-before-bridge-ready");
    const db1 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    // First I/O operation, bridge hasn't been initialized yet.
    const {
      value: { id },
    } = db1.insert(todos, { title: "Test", done: false });

    await waitForCondition(
      async () => {
        const row = await db1.one(allTodos, { tier: "local" });
        return row?.id === id;
      },
      8_000,
      "sync insert should be forwarded to worker after bridge init",
    );

    await db1.shutdown();
    untrack(db1);

    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const persistedRow = await db2.one(allTodos, { tier: "local" });
    expect(persistedRow?.id).toBe(id);
  });

  // -------------------------------------------------------------------------
  // 3. Update + delete through worker bridge
  // -------------------------------------------------------------------------

  it("updates a row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("update") },
      }),
    );

    const { value: inserted } = db.insert(todos, {
      title: "Original",
      done: false,
    });
    const { id } = inserted;
    const result = db.update(todos, id, { done: true });
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });

    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].title).toBe("Original");
    expect(results[0].done).toBe(true);
  });

  it("updates a row durably", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("update-durable") },
      }),
    );

    const { id } = await db
      .insert(todos, { title: "Original", done: false })
      .wait({ tier: "local" });

    const updateHandle = db.update(todos, id, { done: true });
    await updateHandle.wait({ tier: "local" });

    const results = await db.all(allTodos, { tier: "local" });
    expect(results.length).toBe(1);
    expect(results[0].done).toBe(true);
  });

  it("deletes a row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete") },
      }),
    );

    const { value: inserted } = db.insert(todos, {
      title: "Ephemeral",
      done: false,
    });
    const { id } = inserted;
    expect((await db.all(allTodos)).length).toBe(1);

    const result = db.delete(todos, id);
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });
    const results = await db.all(allTodos);
    expect(results.length).toBe(0);
  });

  it("deletes a row durably", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-durable") },
      }),
    );

    const { id } = await db
      .insert(todos, { title: "Ephemeral", done: false })
      .wait({ tier: "local" });
    expect((await db.all(allTodos, { tier: "local" })).length).toBe(1);

    const deleteHandle = db.delete(todos, id);
    await deleteHandle.wait({ tier: "local" });

    const results = await db.all(allTodos, { tier: "local" });
    expect(results.length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 4. IndexedDB persistence across shutdown + re-open
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create", async () => {
    const dbName = uniqueDbName("persistence");

    const db1 = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName },
    });
    db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();

    // A new Db with the same namespace reopens the IndexedDB tree.
    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const after = await db2.all(allTodos, { tier: "local" });
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  it("first local subscription snapshot returns no rows for an empty store", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("empty-snapshot") },
      }),
    );
    const firstSnapshot = new Promise<Todo[]>((resolve) => {
      trackSubscription(db.subscribe(allTodos, resolve, { tier: "local" }));
    });
    await expect(firstSnapshot).resolves.toEqual([]);
  });

  it("first local subscription snapshot contains persisted data", async () => {
    const config = {
      appId: "test-app",
      secret: generateAuthSecret(),
      driver: { type: "persistent" as const, dbName: uniqueDbName("first-snapshot-reopen") },
    };
    const seeded = track(await createDb(config));
    const expected: { id: string; title: string; done: boolean }[] = [];
    for (let index = 0; index < 3; index++) {
      const inserted = seeded.insert(todos, { title: `Persisted ${index}`, done: false });
      const row = await inserted.wait({ tier: "local" });
      expected.push({ id: row.id, title: row.title, done: row.done });
    }
    await seeded.all(allTodos, { tier: "local" });
    await shutdownDbAndWorker(seeded);

    const reopened = track(await createDb(config));
    const snapshots: (typeof expected)[] = [];
    // Subscribe before any read or readiness wait can warm the reopened
    // runtime. An empty callback followed by the stored rows must fail.
    trackSubscription(
      reopened.subscribe(
        todos.orderBy("title"),
        (rows) => snapshots.push(rows.map(({ id, title, done }) => ({ id, title, done }))),
        { tier: "local" },
      ),
    );
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "The initial local snapshot should contain the persisted rows",
    );
    expect(snapshots[0]).toEqual(expected);
  }, 15_000);

  it("deletes IndexedDB storage for the current namespace and keeps the same Db usable", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-storage") },
      }),
    );

    await db.insert(todos, { title: "Should be deleted", done: false }).wait({ tier: "local" });
    const before = await db.all(allTodos, { tier: "local" });
    expect(before.length).toBe(1);
    expect(before[0].title).toBe("Should be deleted");

    await db.deleteClientStorage();

    const afterDelete = await db.all(allTodos, { tier: "local" });
    expect(afterDelete).toEqual([]);

    const {
      value: { id },
    } = db.insert(todos, { title: "Fresh after delete", done: true });
    const afterReinsert = await db.all(allTodos, { tier: "local" });
    expect(afterReinsert).toHaveLength(1);
    expect(afterReinsert[0].id).toBe(id);
    expect(afterReinsert[0].title).toBe("Fresh after delete");
    expect(afterReinsert[0].done).toBe(true);
  });

  it("shuts down immediately after a storage reset and reopens the cleared root", async () => {
    const dbName = uniqueDbName("delete-storage-shutdown");
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    await db.insert(todos, { title: "Before reset", done: false }).wait({ tier: "local" });
    await db.deleteClientStorage();

    await withTimeout(
      db.shutdown(),
      5_000,
      "Db shutdown did not settle immediately after resetting SharedWorker storage",
    );
    untrack(db);

    const reopened = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    expect(await reopened.all(allTodos, { tier: "local" })).toEqual([]);

    await reopened
      .insert(todos, { title: "Fresh after reset and reopen", done: true })
      .wait({ tier: "local" });
    expect(await reopened.all(allTodos, { tier: "local" })).toMatchObject([
      { title: "Fresh after reset and reopen", done: true },
    ]);

    await withTimeout(
      reopened.shutdown(),
      5_000,
      "Reopened Db shutdown did not settle after resetting SharedWorker storage",
    );
    untrack(reopened);
  });

  it("resolves a storage reset requested before any schema use", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-storage-fresh") },
      }),
    );

    // No table/query has run yet: no client exists anywhere in the namespace.
    await db.deleteClientStorage();

    // The same Db must create a fresh shared runtime on first schema use.
    await db
      .insert(todos, { title: "first row after fresh wipe", done: false })
      .wait({ tier: "local" });
    expect(await db.all(allTodos, { tier: "local" })).toHaveLength(1);
  });

  it("resolves a fresh-namespace storage reset while a second fresh tab is open", async () => {
    const dbName = uniqueDbName("delete-storage-fresh-two-tabs");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    // Neither tab has used the schema; both join the reset as participants.
    await dbB.deleteClientStorage();

    // First schema use after the wipe creates the shared runtime; the other
    // fresh tab must attach and observe the write.
    await dbA
      .insert(todos, { title: "row after two-tab fresh wipe", done: false })
      .wait({ tier: "local" });
    await waitForCondition(
      async () => (await dbB.all(allTodos, { tier: "local" })).length === 1,
      8000,
      "Second fresh tab should observe the row written after the wipe",
    );
  });

  it("deletes IndexedDB storage across two tabs when requested by either tab", async () => {
    const dbName = uniqueDbName("delete-storage-two-tabs");
    const dbA = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const dbB = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    await dbA
      .insert(todos, { title: "First tab data before wipe", done: false })
      .wait({ tier: "local" });
    await dbB
      .insert(todos, {
        title: "Second tab data before wipe",
        done: true,
      })
      .wait({ tier: "local" });

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        return firstRows.length === 2 && secondRows.length === 2;
      },
      8000,
      "Both tabs should observe pre-wipe rows",
    );

    await dbB.deleteClientStorage();

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        return firstRows.length === 0 && secondRows.length === 0;
      },
      12000,
      "A storage wipe should clear both tabs",
    );

    const marker = `fresh-after-two-tab-wipe-${Date.now()}`;
    await dbA.insert(todos, { title: marker, done: false }).wait({ tier: "local" });

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        const firstHas = firstRows.some((row) => row.title === marker);
        const secondHas = secondRows.some((row) => row.title === marker);
        return firstHas && secondHas;
      },
      12000,
      "Both tabs should recover cleanly after two-tab storage wipe",
    );
  });

  it("reloads every attached tab when IndexedDB is externally deleted with dirty writes", async () => {
    const dbName = uniqueDbName("external-indexeddb-delete");
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("external-indexeddb-delete-page"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId: "test-app",
      dbName,
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      initialize: true,
      tabCount: 2,
      initialRow: { title: "dirty before external deletion", done: false },
    });

    await deleteRemoteBrowserIndexedDbAndWaitForReload(
      remoteDbId,
      resolveDefaultPersistentDbName({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
  });

  it("logout with wipeData clears browser storage before the next session opens", async () => {
    const dbName = uniqueDbName("logout-wipe");
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    await db
      .insert(todos, { title: "Should be wiped on logout", done: false })
      .wait({ tier: "local" });
    expect((await db.all(allTodos, { tier: "local" })).length).toBe(1);

    await db.logout({ wipeData: true });
    untrack(db);

    const reopened = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const rows = await reopened.all(allTodos, { tier: "local" });
    expect(rows).toEqual([]);
  });

  it("rehydrates current catalogue schema and lens state after persistent worker reopen", async () => {
    const protocolErrors: string[] = [];
    const recordProtocolError = (event: ErrorEvent) => {
      const message = event.error instanceof Error ? event.error.message : event.message;
      if (message.includes("invalid catalogue update")) {
        protocolErrors.push(message);
      }
    };
    globalThis.addEventListener("error", recordProtocolError);
    errorListeners.add(recordProtocolError);

    const dbName = uniqueDbName("catalogue-current-schema-rehydrate");
    const testingServer = await publishCatalogueSchemaFamily("catalogue-current-schema-rehydrate");
    const jwtToken = await getJazzServerJwtForUser(
      "catalogue-current-schema-rehydrate",
      undefined,
      testingServer.appId,
    );

    const seeded = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        jwtToken,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );

    const marker = `catalogue-current-schema-rehydrate-${Date.now()}`;
    await seeded
      .insert(catalogueTodos, {
        title: marker,
        completed: false,
        description: "written with the current schema",
      })
      .wait({ tier: "global" });

    await waitForCatalogueTodos(
      seeded,
      (rows) => rows.some((row) => row.title === marker && row.description?.includes("current")),
      "initial current-schema query should read the persisted row",
      15_000,
      "local",
    );

    await seeded.shutdown();
    untrack(seeded);

    const reopened = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        jwtToken,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );

    const rowsAfterReopen = await waitForCatalogueTodos(
      reopened,
      (rows) => rows.some((row) => row.title === marker && row.description?.includes("current")),
      "reopened persistent worker should rehydrate current schema and lenses before querying",
      15_000,
      "local",
    );
    expect(rowsAfterReopen.find((row) => row.title === marker)?.completed).toBe(false);

    const remote = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        jwtToken,
        registerJwt: true,
        driver: { type: "persistent", dbName: uniqueDbName("catalogue-remote-authority") },
      }),
    );
    const remoteMarker = `catalogue-remote-authority-${Date.now()}`;
    await remote
      .insert(catalogueTodos, {
        title: remoteMarker,
        completed: true,
        description: "written by an independent server-connected client",
      })
      .wait({ tier: "global" });

    const authoritativeRows = await waitForCatalogueTodos(
      reopened,
      (rows) => rows.some((row) => row.title === remoteMarker && row.completed),
      "reopened worker should receive authoritative current-schema rows from the server",
      15_000,
      "global",
    );
    expect(authoritativeRows.find((row) => row.title === remoteMarker)?.description).toContain(
      "independent",
    );

    await sleep(100);
    expect(protocolErrors).toEqual([]);
    globalThis.removeEventListener("error", recordProtocolError);
    errorListeners.delete(recordProtocolError);
  }, 60_000);

  it.runIf(__JAZZ_BROWSER_SOAK__ === "1")(
    "survives repeated durable writes across fresh SharedWorker lifecycles",
    async () => {
      for (let round = 0; round < 24; round += 1) {
        const db = track(
          await createDb({
            appId: "test-app",
            driver: {
              type: "persistent",
              dbName: uniqueDbName(`durable-lifecycle-soak-${round}`),
            },
          }),
        );
        const inserted = await db
          .insert(todos, { title: `durable-${round}`, done: false })
          .wait({ tier: "local" });
        await db.update(todos, inserted.id, { done: true }).wait({ tier: "local" });
        expect(await db.all(allTodos, { tier: "local" })).toEqual([{ ...inserted, done: true }]);
        await db.shutdown();
        untrack(db);
      }
    },
    180_000,
  );

  it.runIf(__JAZZ_BROWSER_SOAK__ === "1")(
    "survives randomized concurrent writes and SharedWorker restarts without cardinality drift",
    async () => {
      const seed = 0x5eed_1703;
      let randomState = seed;
      const random = () => {
        randomState += 0x6d2b_79f5;
        let value = randomState;
        value = Math.imul(value ^ (value >>> 15), value | 1);
        value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
        return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
      };
      const remoteDbId = trackRemoteBrowserDb(uniqueDbName("worker-restart-soak"));
      await withTimeout(
        createRemoteBrowserDb({
          id: remoteDbId,
          appId: "test-app",
          dbName: uniqueDbName("worker-restart-soak-store"),
          table: "todos",
          schemaJson: JSON.stringify(app.wasmSchema),
          tabCount: 3,
          initialize: true,
        }),
        20_000,
        "Soak initial three-tab open timed out",
      );
      const expectedTitles = new Set<string>();
      for (let round = 0; round < 12; round += 1) {
        const writes = Array.from({ length: 3 + Math.floor(random() * 7) }, (_, index) => ({
          row: {
            title: `soak-${seed.toString(16)}-${round}-${index}-${Math.floor(random() * 1e9)}`,
            done: random() < 0.5,
          },
          tabIndex: Math.floor(random() * 3),
        }));
        writes.forEach(({ row }) => expectedTitles.add(row.title));
        await withTimeout(
          Promise.all(
            writes.map(({ row, tabIndex }) => insertRemoteBrowserDbRow(remoteDbId, tabIndex, row)),
          ),
          20_000,
          `Soak round ${round} writes timed out`,
        );
        await waitForCondition(
          async () =>
            (await queryRemoteBrowserDbRows(remoteDbId, round % 3)).length === expectedTitles.size,
          10_000,
          `Soak round ${round} should converge before restart`,
        );
        try {
          await withTimeout(
            restartRemoteBrowserDb(remoteDbId),
            20_000,
            `Soak round ${round} worker restart timed out`,
          );
        } catch (error) {
          throw new Error(`Soak restart failed in round ${round}`, { cause: error });
        }
        const snapshots = await withTimeout(
          Promise.all([0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex))),
          20_000,
          `Soak round ${round} snapshots timed out`,
        );
        for (const snapshot of snapshots) {
          expect(snapshot).toHaveLength(expectedTitles.size);
          expect(new Set(snapshot.map((row) => row.title))).toEqual(expectedTitles);
        }
      }
    },
    180_000,
  );
});

/// <reference types="vite/client" />

/**
 * Durable foreground leases, physical-root fencing and worker admission.
 */

import { describe, it, expect, vi } from "vitest";

import { createBrowserTestDb as createDb, uniqueDbName, withTimeout } from "./support.js";

import { resolveDefaultPersistentDbName } from "../../src/runtime/db.js";

import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";

import { IndexedDbPageStore } from "../../src/runtime/indexeddb-page-store.js";

import {
  createBrowserSharedWorkerBaseName,
  SharedBrowserForegroundNodeLease,
} from "../../src/runtime/native-runtime/browser-shared-worker-connection.js";

import { NativeRuntimeAdapter } from "../../src/runtime/native-runtime/native-runtime-adapter.js";

import { createOpenTransactionId } from "../../src/runtime/client.js";

import { loadWasmModule } from "../../src/runtime/wasm-loader.js";

import { createBrowserStorageOwner } from "../../src/runtime/browser-worker-config.js";

import {
  BrowserWorkerUnresponsiveError,
  serializeBrowserRelayError,
  type BrowserForegroundNodeLeaseAcquireRequest,
  type BrowserForegroundNodeLeasePortRequest,
  type BrowserForegroundNodeLeaseProbeRequest,
  deserializeBrowserRelayError,
  type BrowserRelayError,
} from "../../src/runtime/native-runtime/browser-worker-protocol.js";

import {
  app,
  allTodos,
  startRawForegroundLease,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

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
          store.close();
          await IndexedDbPageStore.destroy(dbName);
        },
      };
    } catch (error) {
      channel.port1.close();
      channel.port2.close();
      await runtime?.close();
      store.close();
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
        reopened.close();
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
        reopened.close();
      }
    } finally {
      await fixture.dispose();
    }
  }, 15_000);
});

describe("SharedWorker bridge with IndexedDB", () => {
  const { track, untrack } = useSharedWorkerBridgeHarness();
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

    // The cancellation acknowledgement precedes successor acquisition below.

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
        IndexedDbPageStore.destroy(dbName),
        5_000,
        "External IndexedDB invalidation remained blocked by the lease-only worker handle",
      );

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
});

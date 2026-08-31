import type { WasmDb } from "jazz-wasm";
import { loadWasmModule, type WasmModule } from "../runtime/client.js";
import { IndexedDbPageStore } from "../runtime/indexeddb-page-store.js";
import {
  acquireBrowserPhysicalDatabaseEpoch,
  BrowserPhysicalDatabaseBusyError,
  type BrowserPhysicalDatabaseEpoch,
} from "../runtime/browser-physical-database-epoch.js";
import { installWasmTelemetry } from "../runtime/sync-telemetry.js";
import {
  BrowserWorkerTransportPump,
  transferableFrames,
} from "../runtime/native-runtime/browser-worker-transport.js";
import {
  serializeBrowserRelayError,
  type BrowserForegroundNodeLeaseAcquireRequest,
  type BrowserForegroundNodeLeaseAcquireResponse,
  type BrowserForegroundNodeLeaseCancelRequest,
  type BrowserForegroundNodeLeaseProbeRequest,
  type BrowserForegroundNodeLeasePortEvent,
  type BrowserForegroundNodeLeasePortRequest,
  type BrowserFollowerPortEvent,
  type BrowserFollowerPortRequest,
  type BrowserInspectorControlEvent,
  type BrowserInspectorControlRequest,
  type BrowserWorkerLifecycleTrace,
  type BrowserSharedWorkerConnectRequest,
  type BrowserSharedWorkerConnectResponse,
  type BrowserWorkerInitOptions,
} from "../runtime/native-runtime/browser-worker-protocol.js";
// Worker failures cross a MessagePort boundary, so retain enough WASM frames to
// identify the Rust call site before serializing them for the owning tab.
(Error as ErrorConstructor & { stackTraceLimit?: number }).stackTraceLimit = 50;
import { openConfig } from "../runtime/native-runtime/native-codec.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { deliverMutationErrorToAttachedPeers } from "./mutation-error-delivery.js";

const DEFAULT_WASM_LOG_LEVEL = "warn";

type SharedWorkerGlobal = typeof globalThis & {
  __JAZZ_WASM_LOG_LEVEL?: BrowserWorkerInitOptions["logLevel"];
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
  close(): void;
};

type TabPeer = {
  tabId: string;
  context: RuntimeContext;
  port: MessagePort;
  pump: BrowserWorkerTransportPump | null;
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]> | null;
  pendingFrames: Uint8Array[];
  flushedLocal: boolean;
  flushRequestId: number | null;
  flushPumpComplete: boolean;
  flushObserved: boolean;
  transportWaitAbort: AbortController;
  onMessage: (event: MessageEvent<BrowserFollowerPortRequest>) => void;
  onMessageError: () => void;
};

type RuntimeContext = {
  key: string;
  fingerprint: string;
  options: BrowserWorkerInitOptions;
  peers: Map<string, TabPeer>;
  runtime: NativeRuntimeAdapter | null;
  initialize: Promise<void>;
  closing: Promise<void> | null;
  idleReleaseTimer: ReturnType<typeof setTimeout> | null;
  pageStore: IndexedDbPageStore | null;
  disposePageStoreInvalidation: (() => void) | null;
  disposeTelemetry: (() => void) | null;
  disposeAuxiliaryTrace: (() => void) | null;
  resetBarrier: { id: number; pending: Set<string>; resolve: () => void } | null;
  storageInvalidated: boolean;
  intentionalStorageReset: boolean;
  serverUrl: string | null;
  serverAuthJson: string;
  serverConnectionStarted: boolean;
  explicitlyDisconnected: boolean;
  transportTransition: Promise<void>;
  transportStateEpoch: number;
  transportStateWaiters: Set<() => void>;
};

type ForegroundLeaseOwner = {
  pageStore: IndexedDbPageStore;
  storageOwner: string;
  activeLeaseIds: Set<string>;
  pendingLeaseAllocations: number;
  pendingLeaseFinalizations: number;
  allocationTail: Promise<void>;
};

type PhysicalDatabaseOwner = {
  epoch: BrowserPhysicalDatabaseEpoch;
  pageStore: IndexedDbPageStore;
  storageOwner: string;
  release: Promise<void> | null;
  disposeInvalidation: (() => void) | null;
};

/**
 * Test-only scheduling/observation injected by a dedicated test worker entry.
 * The production entry never supplies this object and therefore never reads
 * any test-shaped fields from its message protocol.
 */
export type ForegroundLeaseTestHooks = {
  delayBeforeLeaseAllocation(request: BrowserForegroundNodeLeaseAcquireRequest): number | undefined;
  allocationQueued(port: MessagePort): void;
  delayAfterLeaseAllocation(request: BrowserForegroundNodeLeaseAcquireRequest): number | undefined;
  allocationCommitted(port: MessagePort, node: Uint8Array, workerRealmId: string): void;
  cancellationRetired(
    pageStore: IndexedDbPageStore,
    node: Uint8Array,
  ): Promise<"active" | "reusable" | "retired" | "missing">;
};

export type JazzBrokerWorkerOptions = {
  foregroundLeaseTestHooks?: ForegroundLeaseTestHooks;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const workerRealmId = crypto.randomUUID();
let foregroundLeaseTestHooks: ForegroundLeaseTestHooks | null = null;
const contexts = new Map<string, RuntimeContext>();
const foregroundLeaseOwners = new Map<string, ForegroundLeaseOwner>();
const physicalDatabaseOwners = new Map<string, PhysicalDatabaseOwner>();
const physicalDatabaseOwnerAdmissions = new Map<string, Promise<PhysicalDatabaseOwner>>();
const inspectorControlPorts = new Set<MessagePort>();
let wasmModulePromise: Promise<WasmModule> | null = null;
let wasmModuleSource: string | null = null;
let contextInitializationTail: Promise<void> = Promise.resolve();
let nextResetId = 1;
// A port has joined this worker but has not yet completed its first durable
// operation.  It is already a liveness claim: closing the realm in this gap
// can strand a first lease or context connection with no terminal reply.
let pendingBootstrapOperations = 0;
// `workerGlobal.close()` is necessarily deferred behind physical-owner
// release.  A new port may arrive while that release is in flight, so the
// deferred close needs an invalidatable token rather than an unconditional
// `finally(() => close())`.
let pendingWorkerClose: symbol | null = null;
// Inspector-directed termination acknowledges before calling `close()` in a
// later task so the reply can cross its MessagePort. Reject new ports in that
// acknowledgement gap: admitting them into the doomed realm can otherwise
// strand a foreground lease request with no terminal response.
let workerTerminationScheduled = false;
// The inspector-facing trace intentionally does not serialize this key. It
// is only an in-worker capability boundary that prevents a later auth scope
// from observing a prior scope's retained diagnostics for the same realm.
type WorkerLifecycleLedgerEntry = BrowserWorkerLifecycleTrace & {
  authSessionKey: string | null;
};
const workerLifecycleLedger: WorkerLifecycleLedgerEntry[] = [];
let nextWorkerLifecycleSequence = 1;
const MAX_WORKER_LIFECYCLE_ENTRIES = 128;

/**
 * Lifecycle evidence is retained at worker-realm scope, but inspection is a
 * session capability. In particular, a physical database name is not itself
 * an authorization boundary: an auth turnover can retain old entries while
 * the same root becomes relevant to a later scope. Keep the scope check
 * separate from the root check, and never serialize the internal scope key.
 *
 * @internal Exported from this worker-private module for a deterministic
 * boundary receipt; it is not part of the Jazz application API.
 */
export function filterWorkerLifecycleEntriesForInspector(
  entries: readonly WorkerLifecycleLedgerEntry[],
  authSessionKey: string,
  allowedDbNames: ReadonlySet<string>,
): BrowserWorkerLifecycleTrace[] {
  return entries
    .filter((entry) => entry.authSessionKey === authSessionKey && allowedDbNames.has(entry.dbName))
    .map(({ authSessionKey: _authSessionKey, ...entry }) => entry);
}

function recordWorkerLifecycle(
  event: BrowserWorkerLifecycleTrace["event"],
  dbName: string,
  authSessionKey: string | null,
  details: Pick<
    BrowserWorkerLifecycleTrace,
    "frameCount" | "peerActivityEpoch" | "peerProcessedActivityEpoch"
  > = {},
): void {
  const activeLeases = [...foregroundLeaseOwners.values()].reduce(
    (count, owner) => count + owner.activeLeaseIds.size,
    0,
  );
  workerLifecycleLedger.push({
    sequence: nextWorkerLifecycleSequence++,
    event,
    dbName,
    authSessionKey,
    peerCount: [...contexts.values()].reduce((count, context) => count + context.peers.size, 0),
    pendingBootstraps: pendingBootstrapOperations,
    activeLeases,
    ...details,
  });
  if (workerLifecycleLedger.length > MAX_WORKER_LIFECYCLE_ENTRIES) {
    workerLifecycleLedger.splice(0, workerLifecycleLedger.length - MAX_WORKER_LIFECYCLE_ENTRIES);
  }
}

/**
 * Open a physical root only after holding its origin-wide liveness fence.
 *
 * This deliberately lives outside `contexts`: worker assets/generations can
 * create distinct SharedWorker realms, but must never concurrently recover
 * the same durable foreground-lease pool.
 */
async function ensurePhysicalDatabaseOwner(
  dbName: string,
  storageOwner: string,
): Promise<PhysicalDatabaseOwner> {
  const existing = physicalDatabaseOwners.get(dbName);
  if (existing) {
    // A follower may arrive in the small clean-close window. Never hand it a
    // page store whose epoch is already being released; wait for the release
    // and claim a fresh physical epoch instead.
    if (existing.release) {
      await existing.release;
      return await ensurePhysicalDatabaseOwner(dbName, storageOwner);
    }
    if (existing.storageOwner !== storageOwner) {
      throw new Error(
        `IndexedDB database ${dbName} is already owned by a different Jazz browser session; choose a different driver.dbName or reset this database before changing accounts`,
      );
    }
    return existing;
  }

  const pending = physicalDatabaseOwnerAdmissions.get(dbName);
  if (pending) {
    const owner = await pending;
    if (owner.storageOwner !== storageOwner) {
      throw new Error(
        `IndexedDB database ${dbName} is already owned by a different Jazz browser session; choose a different driver.dbName or reset this database before changing accounts`,
      );
    }
    return owner;
  }

  // Publish the same-realm admission before its first await can yield. Two
  // first tabs may enter this function in adjacent worker message turns; Web
  // Locks must fence other worker realms, not make one of those local callers
  // spuriously observe this realm as a competing durable owner.
  const admission = openPhysicalDatabaseOwner(dbName, storageOwner);
  physicalDatabaseOwnerAdmissions.set(dbName, admission);
  try {
    return await admission;
  } finally {
    if (physicalDatabaseOwnerAdmissions.get(dbName) === admission) {
      physicalDatabaseOwnerAdmissions.delete(dbName);
    }
  }
}

async function openPhysicalDatabaseOwner(
  dbName: string,
  storageOwner: string,
): Promise<PhysicalDatabaseOwner> {
  const epoch = await acquireBrowserPhysicalDatabaseEpoch(dbName);
  let pageStore: IndexedDbPageStore | null = null;
  try {
    pageStore = await IndexedDbPageStore.open(dbName, { owner: storageOwner });
    await pageStore.claimBrowserWorkerEpoch(epoch.id);
    const owner: PhysicalDatabaseOwner = {
      epoch,
      pageStore,
      storageOwner,
      release: null,
      disposeInvalidation: null,
    };
    physicalDatabaseOwners.set(dbName, owner);
    // Lease-only bootstrap has no RuntimeContext listener yet. The physical
    // owner itself must release this realm's lock when another agent deletes
    // or upgrades the root underneath it.
    owner.disposeInvalidation = pageStore.onInvalidated(() =>
      releaseInvalidatedPhysicalDatabaseOwner(dbName),
    );
    return owner;
  } catch (error) {
    pageStore?.close();
    await epoch.release();
    throw error;
  }
}

async function releasePhysicalDatabaseOwner(dbName: string): Promise<void> {
  const owner = physicalDatabaseOwners.get(dbName);
  if (!owner) return;
  if (!owner.release) {
    owner.release = (async () => {
      try {
        await owner.pageStore.releaseBrowserWorkerEpoch(owner.epoch.id);
      } finally {
        owner.disposeInvalidation?.();
        owner.disposeInvalidation = null;
        owner.pageStore.close();
        await owner.epoch.release();
        physicalDatabaseOwners.delete(dbName);
      }
    })();
  }
  await owner.release;
}

/**
 * An external IDB delete/versionchange invalidates every handle, including a
 * lease-only bootstrap that has no tab context to close. The epoch record may
 * already be gone, but `releasePhysicalDatabaseOwner` still releases the Web
 * Lock in its finally path. Clear active lease bookkeeping first: those IDs
 * belonged to the erased root and must not keep this realm artificially live.
 */
function releaseInvalidatedPhysicalDatabaseOwner(dbName: string): void {
  foregroundLeaseOwners.delete(dbName);
  void releasePhysicalDatabaseOwner(dbName)
    .catch(() => undefined)
    .finally(() => maybeCloseWorker());
}

export function installJazzBrokerWorker(options: JazzBrokerWorkerOptions = {}): void {
  foregroundLeaseTestHooks = options.foregroundLeaseTestHooks ?? null;
  workerGlobal.onconnect = (event) => {
    const port = event.ports[0];
    if (!port) return;
    // A SharedWorker can accept this port while a previous idle close is
    // awaiting IndexedDB/Web-Lock release. Treat delivery of the port as a
    // new liveness claim before any message task can run; otherwise that old
    // close can terminate this exact admission mid-flight. Do not merely
    // cancel the old close token here: the bootstrap reservation is the
    // durable fact that remains true until this port has either completed its
    // first operation or failed it.
    pendingBootstrapOperations += 1;
    let bootstrapFinished = false;
    let bootstrapPortClosed = false;
    let probedLeaseAttemptId: string | null = null;
    const finishBootstrap = () => {
      if (bootstrapFinished) return;
      bootstrapFinished = true;
      pendingBootstrapOperations -= 1;
      maybeCloseWorker();
    };
    const detachBootstrapListeners = () => {
      port.removeEventListener("message", onBootstrapMessage);
      port.removeEventListener("messageerror", onBootstrapMessageError);
    };
    const closeBootstrapPort = () => {
      if (bootstrapPortClosed) return;
      bootstrapPortClosed = true;
      detachBootstrapListeners();
      finishBootstrap();
      port.close();
    };
    function onBootstrapMessage(
      messageEvent: MessageEvent<
        | BrowserSharedWorkerConnectRequest
        | BrowserForegroundNodeLeaseProbeRequest
        | BrowserForegroundNodeLeaseAcquireRequest
        | BrowserForegroundNodeLeaseCancelRequest
      >,
    ) {
      const message = messageEvent.data;
      if (workerTerminationScheduled) {
        // This is the inspector's acknowledged, intentionally one-way
        // termination handoff. It is deliberately narrower than
        // `pendingWorkerClose`: an ordinary idle close remains cancelable by
        // the bootstrap reservation created in `onconnect`.
        if (message?.type === "connect-runtime") {
          post(port, { type: "worker-closing" } satisfies BrowserSharedWorkerConnectResponse);
        } else if (
          message?.type === "probe-foreground-node-lease-worker" ||
          message?.type === "acquire-foreground-node-lease"
        ) {
          post(port, {
            type: "foreground-node-lease-worker-closing",
            attemptId: message.attemptId ?? "",
          } satisfies BrowserForegroundNodeLeaseAcquireResponse);
        }
        closeBootstrapPort();
        return;
      }
      if (message?.type === "probe-foreground-node-lease-worker") {
        probedLeaseAttemptId = message.attemptId;
        post(port, {
          type: "foreground-node-lease-worker-alive",
          attemptId: message.attemptId,
        } satisfies BrowserForegroundNodeLeaseAcquireResponse);
        return;
      }
      if (message?.type === "cancel-foreground-node-lease") {
        closeBootstrapPort();
        return;
      }
      if (
        message?.type !== "connect-runtime" &&
        message?.type !== "acquire-foreground-node-lease"
      ) {
        return;
      }
      detachBootstrapListeners();
      if (message.type === "connect-runtime") {
        recordWorkerLifecycle(
          "bootstrap-start",
          message.options.dbName,
          message.options.authSessionKey,
        );
        post(port, { type: "worker-alive" });
        void connectTab(port, message, finishBootstrap);
      } else {
        if (probedLeaseAttemptId !== null && message.attemptId !== probedLeaseAttemptId) {
          finishBootstrap();
          post(port, {
            type: "foreground-node-lease-error",
            error: serializeBrowserRelayError(
              new Error("Foreground node lease request did not match its worker probe"),
            ),
          } satisfies BrowserForegroundNodeLeaseAcquireResponse);
          closeBootstrapPort();
          return;
        }
        recordWorkerLifecycle("lease-request", message.dbName, null);
        void acquireForegroundNodeLease(port, message).finally(finishBootstrap);
      }
    }
    function onBootstrapMessageError() {
      closeBootstrapPort();
    }
    port.addEventListener("message", onBootstrapMessage);
    port.addEventListener("messageerror", onBootstrapMessageError);
    port.start();
  };
}

async function acquireForegroundNodeLease(
  port: MessagePort,
  request: BrowserForegroundNodeLeaseAcquireRequest,
): Promise<void> {
  const testHooks = foregroundLeaseTestHooks;
  let owner: ForegroundLeaseOwner | null = null;
  let lease: Awaited<ReturnType<IndexedDbPageStore["acquireForegroundNodeLease"]>> | null = null;
  let allocationPromise: Promise<
    Awaited<ReturnType<IndexedDbPageStore["acquireForegroundNodeLease"]>>
  > | null = null;
  let cancellationRequested = false;
  let cancellationCompletion: Promise<void> | null = null;
  let settled = false;

  const cleanup = () => {
    port.removeEventListener("message", onMessage);
    port.removeEventListener("messageerror", onMessageError);
  };
  const finishCancellation = async (admissionError?: unknown): Promise<void> => {
    if (settled) return;
    // The allocation can finish after the client has requested cancellation.
    // Never publish that identity. If it exists, retire it durably before
    // acknowledging cancellation so the next foreground cannot accumulate
    // abandoned active lease records.
    // A lease-pool transaction may already be in flight while `lease` is
    // still null. Wait for it before acknowledging cancellation: it can
    // commit a durable identity after this message handler starts.
    // A cancel can arrive before physical-owner admission resolves. If that
    // admission rejects, there is no owner or lease to retire, but the client
    // still needs its terminal cancellation receipt to release the retained
    // cleanup port. Do not leave this as a no-op: it permanently coalesces
    // later foreground opens behind a cleanup that can never complete.
    if (!owner) {
      if (admissionError === undefined) return;
      settled = true;
      post(port, {
        type: "foreground-node-lease-cancelled",
        error: serializeBrowserRelayError(admissionError),
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
      cleanup();
      port.close();
      maybeCloseWorker();
      return;
    }
    await allocationPromise?.catch(() => undefined);
    if (settled) return;
    // During an IndexedDB open cancellation can arrive after the physical
    // owner exists but before the lease-pool transaction has started. There
    // is no identity to retire in that case, but the lease-only worker still
    // has to release its physical-root epoch before acknowledging the cancel.
    if (!lease) {
      settled = true;
      post(port, {
        type: "foreground-node-lease-cancelled",
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
      cleanup();
      port.close();
      maybeCloseWorker();
      return;
    }
    settled = true;
    try {
      await retireForegroundNodeLease(owner, lease.leaseId);
      const testLeaseState = testHooks
        ? await testHooks.cancellationRetired(owner.pageStore, lease.node)
        : undefined;
      post(port, {
        type: "foreground-node-lease-cancelled",
        ...(testLeaseState === undefined ? {} : { testLeaseState }),
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
    } catch (error) {
      // A failed retirement remains durably active and therefore fails closed;
      // do not claim that cancellation made the node reusable.
      post(port, {
        type: "foreground-node-lease-cancelled",
        error: serializeBrowserRelayError(
          new Error("Shared browser foreground lease cancellation failed", {
            cause: asError(error),
          }),
        ),
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
    } finally {
      cleanup();
      port.close();
      maybeCloseWorker();
    }
  };
  const requestCancellation = (admissionError?: unknown) => {
    if (settled) return cancellationCompletion;
    cancellationRequested = true;
    // Before an owner exists, the admission continuation calls us again after
    // it has opened the physical root. An admission rejection is the other
    // terminal continuation and must produce the same one-shot receipt.
    if ((owner || admissionError !== undefined) && !cancellationCompletion) {
      cancellationCompletion = finishCancellation(admissionError);
    }
    return cancellationCompletion;
  };
  const retire = async (): Promise<void> => {
    if (settled || !lease || !owner) return;
    settled = true;
    await retireForegroundNodeLease(owner, lease.leaseId);
  };
  const onMessage = (
    event: MessageEvent<
      BrowserForegroundNodeLeasePortRequest | BrowserForegroundNodeLeaseCancelRequest
    >,
  ) => {
    void (async () => {
      const message = event.data;
      try {
        if (message?.type === "cancel-foreground-node-lease") {
          requestCancellation();
          return;
        }
        if (!lease || !owner || settled || cancellationRequested) return;
        if (message?.type === "return-foreground-node-lease") {
          if (!/^(0|[1-9][0-9]*)$/.test(message.confirmedTxTime)) {
            throw new Error("Invalid foreground node lease high-water");
          }
          const highWater = BigInt(message.confirmedTxTime);
          if (highWater > (1n << 64n) - 1n) {
            throw new Error("Invalid foreground node lease high-water");
          }
          // Do not mark this finished until the durable returned receipt has
          // committed. A failing return must still take the durable-retire
          // path; otherwise an active lease could be silently forgotten.
          await owner.pageStore.returnForegroundNodeLease(lease.leaseId, highWater);
          settled = true;
          owner.activeLeaseIds.delete(lease.leaseId);
          post(port, {
            type: "foreground-node-lease-result",
          } satisfies BrowserForegroundNodeLeasePortEvent);
          cleanup();
          port.close();
          maybeCloseWorker();
          return;
        }
        if (message?.type === "retire-foreground-node-lease") {
          await retire();
          post(port, {
            type: "foreground-node-lease-result",
          } satisfies BrowserForegroundNodeLeasePortEvent);
          cleanup();
          port.close();
        }
      } catch (error) {
        // A failed clean handoff is indistinguishable from an interrupted
        // one. Failed retirement leaves the active record for a later worker
        // bootstrap to retire instead of making it reusable.
        await retire().catch(() => undefined);
        post(port, {
          type: "foreground-node-lease-result",
          error: serializeBrowserRelayError(error),
        } satisfies BrowserForegroundNodeLeasePortEvent);
        cleanup();
        port.close();
      }
    })();
  };
  const onMessageError = () => {
    // A dead client cannot complete clean handoff. If allocation completes
    // later, its continuation sees this flag and retires instead of publishing.
    requestCancellation();
  };
  // Install this before any awaited durable admission. It is the cancellation
  // witness for the gap that previously existed between client timeout and
  // the worker attaching its post-lease lifecycle listener.
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", onMessageError);

  try {
    owner = foregroundLeaseOwners.get(request.dbName) ?? null;
    if (!owner) {
      const physicalOwner = await ensurePhysicalDatabaseOwner(request.dbName, request.storageOwner);
      // Another first-tab admission can have installed the in-memory owner
      // while this request awaited the shared physical-open flight. Recheck
      // before deciding whether the durable pool has no live leases: creating
      // a second owner object here would make both callers run abandoned-lease
      // recovery and let the later allocation retire the earlier live node.
      owner = foregroundLeaseOwners.get(request.dbName) ?? null;
      if (!owner) {
        owner = {
          // Lease bootstrap occurs before a foreground runtime is materialized,
          // but it still opens the physical root. Admit its exact durable owner
          // here, before the request can observe or mutate the lease pool.
          pageStore: physicalOwner.pageStore,
          storageOwner: request.storageOwner,
          activeLeaseIds: new Set(),
          pendingLeaseAllocations: 0,
          pendingLeaseFinalizations: 0,
          allocationTail: Promise.resolve(),
        };
        foregroundLeaseOwners.set(request.dbName, owner);
      }
    }
    if (owner.storageOwner !== request.storageOwner) {
      throw new Error(
        `IndexedDB database ${request.dbName} is already owned by a different Jazz browser session; choose a different driver.dbName or reset this database before changing accounts`,
      );
    }
    if (cancellationRequested) {
      await requestCancellation();
      return;
    }
    const allocated = allocateForegroundNodeLease(owner, request, port, testHooks);
    allocationPromise = (async () => {
      const allocatedLease = await allocated;
      // This is an internal browser-receipt seam. It delays only delivery of
      // an already-durable allocation so the test can cancel in the exact
      // window where a lease exists but this handler has not observed it yet.
      const delay = testHooks?.delayAfterLeaseAllocation(request);
      if (delay !== undefined) {
        if (!Number.isSafeInteger(delay) || delay < 0 || delay > 1_000) {
          throw new Error("Invalid foreground lease test delay");
        }
        testHooks?.allocationCommitted(port, allocatedLease.node.slice(), workerRealmId);
        await new Promise<void>((resolve) => setTimeout(resolve, delay));
      }
      return allocatedLease;
    })();
    lease = await allocationPromise;
    if (cancellationRequested) {
      await requestCancellation();
      return;
    }
    recordWorkerLifecycle("lease-admitted", request.dbName, null);
    post(port, {
      type: "foreground-node-lease-ready",
      leaseId: lease.leaseId,
      node: lease.node,
      confirmedTxTime: lease.confirmedTxTime.toString(),
    } satisfies BrowserForegroundNodeLeaseAcquireResponse);
  } catch (error) {
    if (cancellationRequested) {
      await requestCancellation(error);
      return;
    }
    cleanup();
    if (error instanceof BrowserPhysicalDatabaseBusyError) {
      // A Web-Lock conflict means this request has not acquired a durable
      // owner or lease identity. The page may retry it on the same generation;
      // every other bootstrap error remains terminal and causal.
      post(port, {
        type: "foreground-node-lease-busy",
        message: error.message,
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
    } else {
      post(port, {
        type: "foreground-node-lease-error",
        error: serializeBrowserRelayError(error),
      } satisfies BrowserForegroundNodeLeaseAcquireResponse);
    }
    port.close();
  }
}

async function allocateForegroundNodeLease(
  owner: ForegroundLeaseOwner,
  request: BrowserForegroundNodeLeaseAcquireRequest,
  port: MessagePort,
  testHooks: ForegroundLeaseTestHooks | null,
): Promise<Awaited<ReturnType<IndexedDbPageStore["acquireForegroundNodeLease"]>>> {
  // Reservation is synchronous and precedes the queue wait. A lease request
  // is worker-owned lifecycle work from this point onward even though it has
  // not yet published an active durable identity.
  owner.pendingLeaseAllocations += 1;
  const predecessor = owner.allocationTail;
  let release!: () => void;
  owner.allocationTail = new Promise<void>((resolve) => {
    release = resolve;
  });
  await predecessor;
  try {
    const delay = testHooks?.delayBeforeLeaseAllocation(request);
    if (delay !== undefined) {
      if (!Number.isSafeInteger(delay) || delay < 0 || delay > 1_000) {
        throw new Error("Invalid foreground lease test delay");
      }
      testHooks?.allocationQueued(port);
      await new Promise<void>((resolve) => setTimeout(resolve, delay));
    }
    const lease = await owner.pageStore.acquireForegroundNodeLease(owner.activeLeaseIds.size === 0);
    // Publish the live lease before releasing the next allocation. IndexedDB
    // already serializes the durable transactions; this matching in-memory
    // order prevents a queued caller from treating the just-committed lease
    // as abandoned during first-realm recovery.
    owner.activeLeaseIds.add(lease.leaseId);
    return lease;
  } finally {
    owner.pendingLeaseAllocations -= 1;
    release();
    // On success the active ID now retains the worker. On terminal failure no
    // port can finish handoff, so this balanced decrement may release an idle
    // physical owner. Other queued reservations remain counted independently.
    maybeCloseWorker();
  }
}

async function retireForegroundNodeLease(
  owner: ForegroundLeaseOwner,
  leaseId: string,
): Promise<void> {
  // Finalization becomes worker-owned synchronously, before the active marker
  // is removed. This closes the gap where explicit worker termination could
  // otherwise race a durable retirement that had not committed yet.
  owner.pendingLeaseFinalizations += 1;
  owner.activeLeaseIds.delete(leaseId);
  try {
    await owner.pageStore.retireForegroundNodeLease(leaseId);
  } catch (error) {
    // The durable lease remains active when retirement fails. Restore its
    // in-memory marker so this realm continues to fail closed as well.
    owner.activeLeaseIds.add(leaseId);
    throw error;
  } finally {
    owner.pendingLeaseFinalizations -= 1;
    maybeCloseWorker();
  }
}

async function connectTab(
  port: MessagePort,
  message: BrowserSharedWorkerConnectRequest,
  finishBootstrap: () => void,
): Promise<void> {
  try {
    const key = runtimeKey(message.options);
    let context = contexts.get(key);
    if (context?.idleReleaseTimer) {
      clearTimeout(context.idleReleaseTimer);
      context.idleReleaseTimer = null;
    }
    if (context?.closing) {
      await context.closing;
      context = contexts.get(key);
    }
    // `runtimeKey` intentionally names the physical root alone. Do not let a
    // caller with a stale or forged compatible fingerprint attach a different
    // auth scope to that already-open root: this path bypasses a fresh
    // IndexedDbPageStore.open, so the durable marker cannot perform its usual
    // owner comparison for us.
    if (context && context.options.storageOwner !== message.options.storageOwner) {
      throw new Error(
        `IndexedDB database ${message.options.dbName} is already owned by a different Jazz browser session; choose a different driver.dbName or reset this database before changing accounts`,
      );
    }
    if (context && context.fingerprint !== message.fingerprint) {
      throw new Error("incompatible persistent browser configuration");
    }
    if (!context) {
      context = createContext(key, message.fingerprint, message.options);
      contexts.set(key, context);
    }
    await context.initialize;
    await enqueueTransportTransition(context, () => configureServer(context, message.options));
    attachTab(context, message.tabId, port);
    // The peer now owns a durable runtime context.  Drop the bootstrap
    // reservation before publishing readiness so an immediately closing tab
    // still sees normal idle cleanup ordering.
    finishBootstrap();
    post(port, { type: "runtime-ready" });
  } catch (error) {
    // Failed connection setup has already cleaned any partial context; clear
    // the bootstrap reservation before surfacing the terminal result so its
    // physical owner can be released without a follow-on admission race.
    finishBootstrap();
    post(port, { type: "runtime-error", error: serializeBrowserRelayError(error) });
    port.close();
  }
}

function createContext(
  key: string,
  fingerprint: string,
  options: BrowserWorkerInitOptions,
): RuntimeContext {
  const context: RuntimeContext = {
    key,
    fingerprint,
    options,
    peers: new Map(),
    runtime: null,
    pageStore: null,
    disposePageStoreInvalidation: null,
    disposeTelemetry: null,
    disposeAuxiliaryTrace: null,
    resetBarrier: null,
    storageInvalidated: false,
    intentionalStorageReset: false,
    serverUrl: options.serverUrl ?? null,
    serverAuthJson: options.authJson,
    serverConnectionStarted: false,
    explicitlyDisconnected: false,
    transportTransition: Promise.resolve(),
    transportStateEpoch: 0,
    transportStateWaiters: new Set(),
    initialize: Promise.resolve(),
    closing: null,
    idleReleaseTimer: null,
  };
  const initializeContext = contextInitializationTail.then(() => initialize(context));
  contextInitializationTail = initializeContext.catch(() => undefined);
  context.initialize = initializeContext;
  return context;
}

async function initialize(context: RuntimeContext): Promise<void> {
  let unownedDb: WasmDb | null = null;
  try {
    const { options } = context;
    // Opening the page store is also the durable ownership-admission gate for
    // a derived physical browser root. Keep it before *any* WASM work: a
    // rejected owner must not load or configure the process-wide WASM realm,
    // install telemetry, open a native database, or attach a follower.  In
    // particular, a low-level attempt to open another account's physical root
    // is an ordinary connect rejection, not a partially
    // initialized worker that can affect the rightful owner's next open.
    const physicalOwner = await ensurePhysicalDatabaseOwner(options.dbName, options.storageOwner);
    context.pageStore = physicalOwner.pageStore;
    context.disposePageStoreInvalidation = context.pageStore.onInvalidated(() =>
      handleStorageInvalidation(context),
    );
    workerGlobal.__JAZZ_WASM_LOG_LEVEL = options.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    const wasmModule = await loadWorkerWasmModule(options.runtimeSources);
    context.disposeTelemetry = installWasmTelemetry({
      wasmModule,
      collectorUrl: options.telemetryCollectorUrl,
      appId: options.appId,
      runtimeThread: "worker",
    });
    const node = context.pageStore.replicaNode;
    const schema = encodeSchema(options.schema);
    const proof = options.selfSignedClientProof;
    const config = openConfig(node, options.author, 1, false, options.initialSyncFlushEvery, proof);
    if (proof && typeof wasmModule.WasmDb.openBrowserWithSelfSignedProof !== "function") {
      throw new Error(
        "WASM runtime does not support self-signed client opens; rebuild the matching Jazz WASM artifact",
      );
    }
    unownedDb = proof
      ? await wasmModule.WasmDb.openBrowserWithSelfSignedProof(
          context.pageStore,
          schema,
          config,
          proof.token,
          proof.appId,
          proof.claimedAuthor,
        )
      : await wasmModule.WasmDb.openBrowser(context.pageStore, schema, config);
    const runtime = NativeRuntimeAdapter.fromDb(
      unownedDb as never,
      options.schema,
      node,
      options.author,
      1,
      false,
      { selfSignedClientProof: proof },
    );
    if (!unownedDb?.setRelayAuthoritySessionOwner) {
      throw new Error(
        "Browser worker artifact does not support relay authority-session bindings; rebuild the matching Jazz WASM artifact",
      );
    }
    unownedDb.setRelayAuthoritySessionOwner();
    context.runtime = runtime;
    unownedDb = null;
    context.runtime.onAuthFailure((reason) => broadcast(context, { type: "auth-failure", reason }));
    context.runtime.onMutationError((event) => {
      // A mutation error is a notification for foreground runtimes that are
      // attached now. Durable reconciliation belongs to the worker's database;
      // persisting this event would instead surface an old application's toast
      // to an unrelated future tab.
      deliverMutationErrorToAttachedPeers(context.peers.values(), event, (peer, received) =>
        post(peer.port, { type: "mutation-error", event: received }),
      );
    });
    context.runtime.onServerTransportError((error) => {
      // Transport failures are foreground events, not authority fates or
      // durable notifications. Only peers that have completed admission own a
      // tab runtime capable of rejecting an active remote wait.
      for (const peer of context.peers.values()) {
        if (!peer.subscriber || !peer.pump) continue;
        post(peer.port, { type: "transport-error", error: serializeBrowserRelayError(error) });
      }
    });
    if (options.logLevel === "trace") {
      context.disposeAuxiliaryTrace = context.runtime.onAuxiliaryTrace((entries) => {
        broadcast(context, {
          type: "relay-trace",
          entries: entries.map((entry) => ({ ...entry, hop: "worker-server" })),
        });
      });
    }
  } catch (error) {
    try {
      await unownedDb?.close();
    } catch {
      // The initialisation error is the actionable failure.
    }
    try {
      cleanupFailedContext(context);
    } catch {
      // Cleanup must not replace the initialisation error reported to the tab.
    }
    throw error;
  }
}

async function loadWorkerWasmModule(
  runtimeSources: BrowserWorkerInitOptions["runtimeSources"],
): Promise<WasmModule> {
  const source = workerWasmSource(runtimeSources);
  if (wasmModulePromise && (!source || wasmModuleSource !== source)) {
    throw new Error(
      "incompatible WASM asset source for this SharedWorker; start a worker scoped to the new asset URL",
    );
  }
  const load = wasmModulePromise ?? loadWasmModule(runtimeSources);
  wasmModulePromise = load;
  wasmModuleSource = source;
  try {
    return await load;
  } catch (error) {
    if (wasmModulePromise === load) {
      wasmModulePromise = null;
      wasmModuleSource = null;
    }
    throw error;
  }
}

function workerWasmSource(
  runtimeSources: BrowserWorkerInitOptions["runtimeSources"],
): string | null {
  // The page assigns an opaque identity before structured-cloning an in-memory
  // source into this worker. A raw worker caller without that identity is
  // deliberately unshareable: treating every supplied byte array/module as
  // the same source would silently reuse the first wasm-bindgen realm.
  if (runtimeSources?.wasmModule) {
    return runtimeSources.workerWasmAssetIdentity
      ? `module:${runtimeSources.workerWasmAssetIdentity}`
      : null;
  }
  if (runtimeSources?.wasmSource) {
    return runtimeSources.workerWasmAssetIdentity
      ? `source:${runtimeSources.workerWasmAssetIdentity}`
      : null;
  }
  return runtimeSources?.wasmUrl ?? "worker-local";
}

function cleanupFailedContext(context: RuntimeContext): void {
  const runtime = context.runtime;
  const disposePageStoreInvalidation = context.disposePageStoreInvalidation;
  const disposeTelemetry = context.disposeTelemetry;
  const disposeAuxiliaryTrace = context.disposeAuxiliaryTrace;
  context.runtime = null;
  context.pageStore = null;
  context.disposePageStoreInvalidation = null;
  context.disposeTelemetry = null;
  context.disposeAuxiliaryTrace = null;
  if (contexts.get(context.key) === context) contexts.delete(context.key);
  try {
    runtime?.discard();
  } finally {
    try {
      disposePageStoreInvalidation?.();
    } finally {
      try {
        disposeTelemetry?.();
      } finally {
        disposeAuxiliaryTrace?.();
      }
    }
  }
  maybeCloseWorker();
}

async function configureServer(
  context: RuntimeContext,
  options: BrowserWorkerInitOptions,
): Promise<void> {
  const requestedUrl = options.serverUrl ?? null;
  if (context.serverUrl === requestedUrl) {
    context.serverAuthJson = options.authJson;
    if (context.serverConnectionStarted) await requireRuntime(context).updateAuth(options.authJson);
    return;
  }
  if (context.peers.size > 0) {
    throw new Error("incompatible persistent browser server configuration");
  }
  if (context.serverConnectionStarted) {
    await requireRuntime(context).disconnect({ rejectWaiters: false });
    context.serverConnectionStarted = false;
  }
  context.serverUrl = requestedUrl;
  context.serverAuthJson = options.authJson;
}

function ensureServerConnection(context: RuntimeContext): void {
  const requestedUrl = context.serverUrl;
  if (!requestedUrl || context.explicitlyDisconnected) return;
  if (context.serverConnectionStarted) return;
  requireRuntime(context).connect(requestedUrl, context.serverAuthJson);
  context.serverConnectionStarted = true;
}

/**
 * The durable worker has one upstream transport, while every MessagePort is
 * serviced independently. Serialize mutations and observations of that shared
 * transport so an older disconnect cannot complete after a newer reconnect.
 */
function enqueueTransportTransition(
  context: RuntimeContext,
  transition: () => void | Promise<void>,
): Promise<void> {
  const queued = context.transportTransition.then(transition, transition);
  context.transportTransition = queued.catch(() => undefined);
  return queued;
}

function publishExplicitOffline(context: RuntimeContext): void {
  context.transportStateEpoch += 1;
  broadcast(context, {
    type: "transport-state",
    explicitlyDisconnected: context.explicitlyDisconnected,
  });
  const waiters = [...context.transportStateWaiters];
  context.transportStateWaiters.clear();
  for (const wake of waiters) wake();
}

function waitForTransportStateChange(
  context: RuntimeContext,
  observedEpoch: number,
  signal?: AbortSignal,
): Promise<void> {
  if (context.transportStateEpoch !== observedEpoch || signal?.aborted) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const finish = () => {
      context.transportStateWaiters.delete(finish);
      signal?.removeEventListener("abort", finish);
      resolve();
    };
    signal?.addEventListener("abort", finish, { once: true });
    context.transportStateWaiters.add(finish);
  });
}

async function waitForServerConnection(
  context: RuntimeContext,
  runtime: NativeRuntimeAdapter,
  signal?: AbortSignal,
): Promise<void> {
  for (;;) {
    if (signal?.aborted) return;
    await context.transportTransition;
    if (signal?.aborted) return;
    if (context.explicitlyDisconnected) {
      const epoch = context.transportStateEpoch;
      await waitForTransportStateChange(context, epoch, signal);
      continue;
    }
    await runtime.waitForUpstreamServerConnection();
    // A disconnect can be queued while carrier negotiation is pending. Do not
    // tell a caller that remote readiness succeeded until it observes the
    // resulting namespace state.
    if (!context.explicitlyDisconnected) return;
  }
}

function attachTab(context: RuntimeContext, tabId: string, port: MessagePort): void {
  closeTab(context, tabId);
  let peer!: TabPeer;
  const onMessage = (event: MessageEvent<BrowserFollowerPortRequest>) => {
    void handleTabMessage(peer, event.data);
  };
  const onMessageError = () => closeTab(context, tabId);
  peer = {
    tabId,
    context,
    port,
    pump: null,
    subscriber: null,
    pendingFrames: [],
    flushedLocal: false,
    flushRequestId: null,
    flushPumpComplete: false,
    flushObserved: false,
    transportWaitAbort: new AbortController(),
    onMessage,
    onMessageError,
  };
  context.peers.set(tabId, peer);
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", onMessageError);
  // SharedWorker connection ports are started by `onconnect`, but inspector
  // peers arrive as freshly transferred MessageChannel ports. Starting is
  // idempotent and required before addEventListener-based delivery can begin.
  port.start();
}

async function handleTabMessage(peer: TabPeer, message: BrowserFollowerPortRequest): Promise<void> {
  if (peer.context.peers.get(peer.tabId) !== peer) return;
  if (message.type === "frames") {
    if (peer.context.options.logLevel === "trace") {
      recordWorkerLifecycle(
        "peer-frames",
        peer.context.options.dbName,
        peer.context.options.authSessionKey,
        { frameCount: message.frames.length },
      );
    }
    peer.flushedLocal = false;
    if (!peer.pump) {
      // The init handler may be awaiting an evaluator pass while MessagePort
      // delivery continues. Preserve ordering instead of treating those
      // already-staged follower frames as a protocol violation.
      peer.pendingFrames.push(...message.frames);
      return;
    }
    peer.pump.receive(message.frames);
    return;
  }
  if (message.type === "close") {
    const releaseWhenIdle = peer.context.peers.size === 1 && message.releaseContext;
    if (message.id !== undefined) result(peer, message.id);
    // The requester owns graceful port closure. Closing this endpoint in the
    // same task as the acknowledgement can discard that queued message in
    // WebKit, leaving shutdown pending forever. Detach runtime ownership now;
    // the client closes both ends after it observes the result.
    closeTab(peer.context, peer.tabId, false);
    if (releaseWhenIdle) scheduleIdleContextRelease(peer.context);
    return;
  }
  if (message.type === "diagnostic-query-coverage") {
    recordWorkerLifecycle(
      message.stage === "attach" ? "query-attach" : "query-covered",
      peer.context.options.dbName,
      peer.context.options.authSessionKey,
      {
        peerActivityEpoch: message.peerActivityEpoch,
        peerProcessedActivityEpoch: message.peerProcessedActivityEpoch,
      },
    );
    return;
  }
  if (message.type === "storage-reset-observed") {
    observeStorageReset(peer, message.resetId);
    return;
  }
  if (message.type === "flush-local-observed") {
    peer.flushObserved = true;
    completeLocalFlush(peer);
    return;
  }
  if (message.type === "open-inspector-control") {
    attachInspectorControl(peer.context.options.authSessionKey, message.port);
    result(peer, message.id);
    return;
  }
  if (message.type === "prepare-storage-reset") {
    peer.context.intentionalStorageReset = true;
    result(peer, message.id);
    return;
  }
  if (message.type === "abort-storage-reset") {
    peer.context.intentionalStorageReset = false;
    result(peer, message.id);
    return;
  }
  if (message.type === "finish-storage-reset") {
    await finalizeContextStorageReset(peer.context);
    void notifyStorageReset(peer.context);
    result(peer, message.id);
    return;
  }

  try {
    const activeRuntime = requireRuntime(peer.context);
    if (message.type === "init") {
      if (peer.pump || peer.subscriber) throw new Error("Browser tab is already initialized");
      // Peer admission mutates the connection registry. A running evaluator
      // may hold that registry across storage suspension, so install only at
      // the owner-wide evaluator boundary. Storage progress is independent of
      // this new peer and therefore continues while admission waits.
      const subscriber = await activeRuntime.acceptPeerWhenIdle(message.sessionClaims);
      const pump = attachPeerTransport(peer, activeRuntime, subscriber);
      recordWorkerLifecycle(
        "peer-attached",
        peer.context.options.dbName,
        peer.context.options.authSessionKey,
      );
      if (peer.pendingFrames.length > 0) {
        const pending = peer.pendingFrames.splice(0);
        pump.receive(pending);
      }
      await enqueueTransportTransition(peer.context, () => {
        if (peer.context.explicitlyDisconnected) {
          post(peer.port, { type: "transport-state", explicitlyDisconnected: true });
        } else {
          ensureServerConnection(peer.context);
        }
      });
      result(peer, message.id);
      return;
    }
    if (message.type === "update-auth") {
      if (!peer.subscriber) throw new Error("Browser tab is not initialized");
      await enqueueTransportTransition(peer.context, async () => {
        await peer.subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
        peer.context.serverAuthJson = message.authJson;
        if (!peer.context.explicitlyDisconnected) {
          await activeRuntime.updateAuth(message.authJson);
        }
      });
      broadcast(peer.context, { type: "auth-restored" });
      return;
    }
    if (message.type === "disconnect") {
      await enqueueTransportTransition(peer.context, async () => {
        const wasExplicitlyDisconnected = peer.context.explicitlyDisconnected;
        peer.context.explicitlyDisconnected = true;
        peer.context.serverConnectionStarted = false;
        try {
          await activeRuntime.disconnect({ rejectWaiters: false });
        } catch (error) {
          // Match the public Db contract: a failed explicit disconnect must
          // not silently change RemoteIfPossible behavior for any tab. The
          // adapter may already have detached the old carrier before reporting
          // a retirement error, so restore normal connection ownership.
          peer.context.explicitlyDisconnected = wasExplicitlyDisconnected;
          if (!wasExplicitlyDisconnected) ensureServerConnection(peer.context);
          throw error;
        }
        publishExplicitOffline(peer.context);
      });
      result(peer, message.id);
      return;
    }
    if (message.type === "flush-local") {
      if (peer.flushRequestId !== null) {
        result(peer, message.id, new Error("Browser tab already has a local flush in progress"));
        return;
      }
      peer.flushRequestId = message.id;
      peer.flushPumpComplete = false;
      peer.flushObserved = false;
      peer.pump?.drainOutboundFrames();
      // The page reports `flush-local-observed` only after every pending local
      // settlement handle has resolved. Those handles are acknowledged by the
      // durable worker after persistence, so evaluator quiescence is neither
      // necessary nor relevant to this durability barrier.
      peer.flushPumpComplete = true;
      completeLocalFlush(peer);
      return;
    }
    if (message.type === "reconnect") {
      const serverUrl = peer.context.serverUrl;
      if (!serverUrl) throw new Error("Browser runtime reconnect requires a serverUrl");
      await enqueueTransportTransition(peer.context, async () => {
        await peer.subscriber?.updateAuthenticatedClaims?.(message.sessionClaims);
        peer.context.serverAuthJson = message.authJson;
        activeRuntime.connect(serverUrl, message.authJson);
        peer.context.explicitlyDisconnected = false;
        peer.context.serverConnectionStarted = true;
        publishExplicitOffline(peer.context);
      });
      result(peer, message.id);
      return;
    }
    await waitForServerConnection(peer.context, activeRuntime, peer.transportWaitAbort.signal);
    if (peer.transportWaitAbort.signal.aborted) return;
    result(peer, message.id);
  } catch (error) {
    if ("id" in message) result(peer, message.id, asError(error));
    else failPeer(peer, asError(error));
  }
}

function attachInspectorControl(authSessionKey: string, port: MessagePort): void {
  inspectorControlPorts.add(port);
  const onMessage = (event: MessageEvent<BrowserInspectorControlRequest>) => {
    const message = event.data;
    if (message.type === "close") {
      dispose();
      return;
    }
    if (message.type === "list-contexts") {
      const available = [...contexts.values()]
        .filter(
          (context) =>
            context.options.authSessionKey === authSessionKey &&
            !context.storageInvalidated &&
            context.runtime !== null,
        )
        .map((context) => ({
          workerRealmId,
          key: context.key,
          appId: context.options.appId,
          dbName: context.options.dbName,
          schema: context.options.schema,
        }));
      port.postMessage({
        type: "contexts",
        id: message.id,
        contexts: available,
      } satisfies BrowserInspectorControlEvent);
      return;
    }
    if (message.type === "lifecycle-trace") {
      // An inspector is scoped by its authenticated worker session.  Return
      // only entries for physical roots currently live under that session;
      // lifecycle diagnostics must not become a cross-account root oracle.
      const allowedDbNames = new Set(
        [...contexts.values()]
          .filter(
            (context) =>
              context.options.authSessionKey === authSessionKey &&
              !context.storageInvalidated &&
              context.runtime !== null,
          )
          .map((context) => context.options.dbName),
      );
      port.postMessage({
        type: "lifecycle-trace",
        id: message.id,
        entries: filterWorkerLifecycleEntriesForInspector(
          workerLifecycleLedger,
          authSessionKey,
          allowedDbNames,
        ),
      } satisfies BrowserInspectorControlEvent);
      return;
    }
    if (message.type === "terminate-worker") {
      if (contexts.size > 0) {
        port.postMessage({
          type: "result",
          id: message.id,
          error: "Worker still has live runtime contexts",
        } satisfies BrowserInspectorControlEvent);
        return;
      }
      if (pendingBootstrapOperations > 0) {
        port.postMessage({
          type: "result",
          id: message.id,
          error: "Worker still has pending bootstrap operations",
        } satisfies BrowserInspectorControlEvent);
        return;
      }
      if (hasForegroundLeaseWork()) {
        port.postMessage({
          type: "result",
          id: message.id,
          error: "Worker still has pending or active foreground node leases",
        } satisfies BrowserInspectorControlEvent);
        return;
      }
      workerTerminationScheduled = true;
      port.postMessage({
        type: "result",
        id: message.id,
        workerTerminated: true,
      } satisfies BrowserInspectorControlEvent);
      // Termination happens in a later task so the acknowledgement crosses the
      // MessagePort before this realm disappears. A subsequent connection's
      // bootstrap retry advances to a distinct SharedWorker generation.
      setTimeout(() => workerGlobal.close(), 0);
      return;
    }
    const context = contexts.get(message.contextKey);
    if (
      !context ||
      context.options.authSessionKey !== authSessionKey ||
      context.storageInvalidated ||
      !context.runtime
    ) {
      port.postMessage({
        type: "result",
        id: message.id,
        error: "Inspector context is no longer available",
      } satisfies BrowserInspectorControlEvent);
      return;
    }
    attachTab(context, message.tabId, message.port);
    port.postMessage({ type: "result", id: message.id } satisfies BrowserInspectorControlEvent);
  };
  const dispose = () => {
    port.removeEventListener("message", onMessage);
    port.removeEventListener("messageerror", dispose);
    inspectorControlPorts.delete(port);
    port.close();
    maybeCloseWorker();
  };
  port.addEventListener("message", onMessage);
  port.addEventListener("messageerror", dispose);
  port.start();
}

function result(peer: TabPeer, id: number, error?: Error): void {
  if (peer.context.peers.get(peer.tabId) !== peer) return;
  post(peer.port, {
    type: "result",
    id,
    ...(error ? { error: serializeBrowserRelayError(error) } : {}),
  });
}

function failPeer(peer: TabPeer, error: Error): void {
  post(peer.port, { type: "error", error: serializeBrowserRelayError(error) });
  closeTab(peer.context, peer.tabId);
}

function closeTab(context: RuntimeContext, tabId: string, closePort = true): void {
  const peer = context.peers.get(tabId);
  if (!peer) return;
  context.peers.delete(tabId);
  peer.transportWaitAbort.abort();
  acknowledgeReset(context, tabId);
  peer.port.removeEventListener("message", peer.onMessage);
  peer.port.removeEventListener("messageerror", peer.onMessageError);
  detachPeerRuntime(peer);
  if (closePort) peer.port.close();
}

function detachPeerRuntime(peer: TabPeer): void {
  peer.pump?.close();
  peer.pump = null;
  // The pump exclusively owns logical closure of the WASM transport receiver
  // and defers it until an in-flight evaluator pass has unwound. Do not call
  // `free()` here: Rust futures may retain the wasm-bindgen wrapper beyond the
  // visible peer lifetime, so its registered finalizer owns physical release.
  peer.subscriber = null;
  peer.pendingFrames.length = 0;
}

function broadcast(context: RuntimeContext, event: BrowserFollowerPortEvent): void {
  for (const peer of context.peers.values()) post(peer.port, event);
}

function requireRuntime(context: RuntimeContext): NativeRuntimeAdapter {
  if (!context.runtime) throw new Error("Shared browser runtime is closed");
  return context.runtime;
}

async function finalizeContextStorageReset(context: RuntimeContext): Promise<void> {
  context.intentionalStorageReset = false;
  for (const peer of context.peers.values()) {
    // The persistence epoch is already gone. Do not call into transport or
    // subscriber wrappers whose WASM receiver may still be unwinding the IDB
    // versionchange; abandon them with the discarded runtime instead.
    peer.pump = null;
    peer.subscriber = null;
    peer.pendingFrames.length = 0;
  }
  context.runtime?.discard();
  context.runtime = null;
  context.disposePageStoreInvalidation?.();
  context.disposePageStoreInvalidation = null;
  context.pageStore = null;
  context.disposeTelemetry?.();
  context.disposeTelemetry = null;
  context.disposeAuxiliaryTrace?.();
  context.disposeAuxiliaryTrace = null;
  contexts.delete(context.key);

  // The reset deleted the physical lease pool along with the page tree. Drop
  // the worker's handle to that erased epoch before a successor asks for a
  // lease; otherwise it would try to mutate an invalidated IDB connection.
  // Existing foregrounds are being reset/discarded and their captured owners
  // may only fail closed while returning their now-retired leases.
  const leaseOwner = foregroundLeaseOwners.get(context.options.dbName);
  if (leaseOwner) {
    foregroundLeaseOwners.delete(context.options.dbName);
    leaseOwner.pageStore.close();
  }
  // `deleteStorage` invalidated the durable epoch record with the root. The
  // Web Lock must nevertheless be released so a successor can claim the new
  // physical epoch.
  await releasePhysicalDatabaseOwner(context.options.dbName).catch(() => undefined);
}

async function releaseIdleContext(context: RuntimeContext): Promise<void> {
  if (!context.closing) {
    context.closing = (async () => {
      for (const peer of context.peers.values()) {
        peer.pump?.close();
        peer.pump = null;
        peer.subscriber = null;
        peer.pendingFrames.length = 0;
      }
      // The last peer's flush barrier already drained evaluator persistence.
      // Do not retain a graceful close future after every page has gone: a
      // suspended cold/query lifecycle cannot add durability at this point.
      context.runtime?.discard();
      context.runtime = null;
      context.disposePageStoreInvalidation?.();
      context.disposePageStoreInvalidation = null;
      context.pageStore = null;
      context.disposeTelemetry?.();
      context.disposeTelemetry = null;
      context.disposeAuxiliaryTrace?.();
      context.disposeAuxiliaryTrace = null;
      if (contexts.get(context.key) === context) contexts.delete(context.key);
    })();
  }
  await context.closing;
}

function scheduleIdleContextRelease(context: RuntimeContext): void {
  if (context.idleReleaseTimer) clearTimeout(context.idleReleaseTimer);
  context.idleReleaseTimer = setTimeout(() => {
    context.idleReleaseTimer = null;
    if (context.peers.size !== 0) return;
    void releaseIdleContext(context).then(() => {
      maybeCloseWorker();
    });
  }, 50);
}

function maybeCloseWorker(): void {
  if (!workerHasLiveWork()) {
    if (pendingWorkerClose) return;
    const closeToken = Symbol("worker-idle-close");
    pendingWorkerClose = closeToken;
    const databaseNames = new Set<string>([
      ...foregroundLeaseOwners.keys(),
      ...physicalDatabaseOwners.keys(),
    ]);
    // The physical owner writes/deletes its epoch before closing the shared
    // page-store handle below. Closing lease aliases first would turn an
    // otherwise clean handoff into a stale durable epoch.
    for (const dbName of databaseNames) recordWorkerLifecycle("owner-release-start", dbName, null);
    foregroundLeaseOwners.clear();
    void Promise.all([...databaseNames].map((dbName) => releasePhysicalDatabaseOwner(dbName)))
      .catch(() => undefined)
      // Unit harnesses execute the module outside an actual worker global;
      // production SharedWorkerGlobal always supplies `close`.
      .finally(() => {
        // A new connection may have arrived while IndexedDB/Web Locks were
        // draining. Its bootstrap reservation is established synchronously
        // in `onconnect`, before it can perform its first durable operation.
        // Never close a realm that has acquired that new work.
        if (pendingWorkerClose !== closeToken) return;
        // The release completed while a new port was bootstrapping. It owns
        // the cancellation of this particular close attempt, but not the
        // worker forever: clear the stale token so a failed bootstrap can
        // schedule a fresh idle close in `finishBootstrap`.
        if (workerHasLiveWork()) {
          pendingWorkerClose = null;
          return;
        }
        for (const dbName of databaseNames) {
          recordWorkerLifecycle("owner-release-finished", dbName, null);
        }
        pendingWorkerClose = null;
        workerGlobal.close?.();
      });
  }
}

function workerHasLiveWork(): boolean {
  return (
    contexts.size > 0 ||
    inspectorControlPorts.size > 0 ||
    pendingBootstrapOperations > 0 ||
    hasForegroundLeaseWork()
  );
}

function hasForegroundLeaseWork(): boolean {
  return [...foregroundLeaseOwners.values()].some(
    (owner) =>
      owner.activeLeaseIds.size > 0 ||
      owner.pendingLeaseAllocations > 0 ||
      owner.pendingLeaseFinalizations > 0,
  );
}

function closeContextPeers(context: RuntimeContext): void {
  for (const tabId of context.peers.keys()) closeTab(context, tabId);
}

function handleStorageInvalidation(context: RuntimeContext): void {
  if (context.intentionalStorageReset) {
    context.runtime?.discard();
    context.runtime = null;
    context.pageStore = null;
    return;
  }
  if (context.storageInvalidated) return;
  context.storageInvalidated = true;
  contexts.delete(context.key);
  context.disposePageStoreInvalidation?.();
  context.disposePageStoreInvalidation = null;
  context.pageStore = null;
  context.runtime?.discard();
  context.runtime = null;
  context.disposeTelemetry?.();
  context.disposeTelemetry = null;
  context.disposeAuxiliaryTrace?.();
  context.disposeAuxiliaryTrace = null;
  // Context listeners cover active runtimes; the physical-owner listener also
  // covers lease-only bootstrap. This is idempotent so either ordering safely
  // releases the erased root's epoch/lock.
  releaseInvalidatedPhysicalDatabaseOwner(context.options.dbName);
  broadcast(context, { type: "storage-invalidated" });
  setTimeout(() => closeContextPeers(context), 0);
}

async function notifyStorageReset(context: RuntimeContext): Promise<void> {
  const pending = new Set(context.peers.keys());
  if (pending.size === 0) return;
  const id = nextResetId++;
  await new Promise<void>((resolve) => {
    context.resetBarrier = { id, pending, resolve };
    broadcast(context, { type: "storage-reset", resetId: id });
  });
}

function observeStorageReset(peer: TabPeer, resetId: number): void {
  if (peer.context.resetBarrier?.id !== resetId) return;
  acknowledgeReset(peer.context, peer.tabId);
}

function acknowledgeReset(context: RuntimeContext, tabId: string): void {
  const barrier = context.resetBarrier;
  if (!barrier || !barrier.pending.delete(tabId) || barrier.pending.size !== 0) return;
  context.resetBarrier = null;
  barrier.resolve();
}

function attachPeerTransport(
  peer: TabPeer,
  activeRuntime: NativeRuntimeAdapter,
  subscriber: ReturnType<NativeRuntimeAdapter["acceptPeer"]>,
): BrowserWorkerTransportPump {
  peer.subscriber = subscriber;
  peer.pump = new BrowserWorkerTransportPump(
    activeRuntime,
    subscriber,
    (frames) => {
      const copies = transferableFrames(frames);
      peer.port.postMessage(
        { type: "frames", frames: copies } satisfies BrowserFollowerPortEvent,
        copies.map((frame) => frame.buffer),
      );
    },
    (error) => failPeer(peer, asError(error)),
    peer.context.options.logLevel === "trace"
      ? (entries) => {
          post(peer.port, {
            type: "relay-trace",
            entries: entries.map((entry) => ({ ...entry, hop: "worker-tab" })),
          });
        }
      : undefined,
  );
  return peer.pump;
}

function completeLocalFlush(peer: TabPeer): void {
  const requestId = peer.flushRequestId;
  if (requestId === null || !peer.flushPumpComplete || !peer.flushObserved) return;
  peer.flushRequestId = null;
  peer.flushedLocal = true;
  result(peer, requestId);
}

function runtimeKey(options: BrowserWorkerInitOptions): string {
  // One runtime/page-store owner per physical IndexedDB root. The persistent
  // owner marker covers distinct worker assets that cannot share this realm.
  return options.dbName;
}

function post(
  port: MessagePort,
  event:
    | BrowserFollowerPortEvent
    | BrowserSharedWorkerConnectResponse
    | BrowserForegroundNodeLeaseAcquireResponse
    | BrowserForegroundNodeLeasePortEvent,
): void {
  port.postMessage(event);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

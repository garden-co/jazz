import type { LeaderRole } from "./tab-leader-election.js";
import type {
  StorageResetAckMessage,
  StorageResetBeginMessage,
  StorageResetFinishedMessage,
  StorageResetRequestMessage,
  TabSyncMessage,
} from "./tab-sync-protocol.js";

const STORAGE_RESET_REQUEST_RETRY_MS = 200;
const STORAGE_RESET_REQUEST_TIMEOUT_MS = 5_000;
const STORAGE_RESET_DISCOVERY_WINDOW_MS = 600;
const STORAGE_RESET_ACK_QUIET_MS = 150;

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
};

type StorageResetContext = {
  requestId: string;
  initiatedBySelf: boolean;
  coordinatorTabId: string | null;
  begun: boolean;
  completed: boolean;
  preparePromise: Promise<string> | null;
  completion: Deferred<void>;
};

type CoordinatorState = {
  requestId: string;
  startedAtMs: number;
  lastAckAtMs: number;
  ackedNamespacesByTabId: Map<string, string>;
  runPromise: Promise<void> | null;
};

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createOperationId(prefix: string): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return `${prefix}-${cryptoObj.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function toError(error: unknown, fallbackMessage: string): Error {
  return error instanceof Error ? error : new Error(error ? String(error) : fallbackMessage);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * The capabilities the {@link StorageResetCoordinator} needs from its embedding
 * runtime (the `Db`) to drive a cross-tab OPFS reset: tab/leader state, the
 * sync channel for tab-to-tab messages, and lifecycle hooks to shut down and
 * resume the worker around the destructive delete.
 */
export interface StorageResetHost {
  isShuttingDown(): boolean;
  getTabId(): string | null;
  getTabRole(): LeaderRole;
  getCurrentLeaderTabId(): string | null;
  getCurrentLeaderTerm(): number;
  hasSyncChannel(): boolean;
  getPrimaryDbName(): string | null;
  getCurrentWorkerNamespace(): string;
  postSyncChannelMessage(message: TabSyncMessage): void;
  ensureBridgeReady(): Promise<void>;
  shutdownWorkerAndClients(): Promise<void>;
  resumeWorker(): Promise<void>;
}

/**
 * Coordinates a browser storage (OPFS) reset across all open tabs of the same
 * origin so the destructive delete only happens while every tab has released
 * its worker.
 *
 * The leader tab runs the protocol: it broadcasts `storage-reset-begin`, waits
 * for each follower's `storage-reset-ack` with its current namespace, deletes
 * the matching OPFS files once the channel goes quiet, and broadcasts
 * `storage-reset-finished` so all tabs resume their workers in lockstep.
 * Non-leader tabs send `storage-reset-request` to the leader and otherwise
 * follow the leader's messages. With no sync channel (single-tab fallback)
 * the local tab plays both roles.
 */
export class StorageResetCoordinator {
  private active: StorageResetContext | null = null;
  private coordinator: CoordinatorState | null = null;

  constructor(private readonly host: StorageResetHost) {}

  async requestReset(): Promise<void> {
    if (!this.host.hasSyncChannel() || !this.host.getTabId()) {
      const requestId = createOperationId("storage-reset");
      const context = this.getOrCreateContext(requestId, true);
      try {
        await this.runSingleTab(context);
        this.resolveContext(context);
      } catch (error) {
        this.rejectContext(context, error);
      }
      await context.completion.promise;
      return;
    }

    if (this.active) {
      await this.active.completion.promise;
      return;
    }

    const requestId = createOperationId("storage-reset");
    const context = this.getOrCreateContext(requestId, true);

    if (this.host.getTabRole() === "leader") {
      await this.startAsCoordinator(context);
      return;
    }

    const deadline = Date.now() + STORAGE_RESET_REQUEST_TIMEOUT_MS;
    while (!context.begun) {
      if (this.host.getTabRole() === "leader") {
        await this.startAsCoordinator(context);
        return;
      }

      this.host.postSyncChannelMessage({
        type: "storage-reset-request",
        requestId,
        fromTabId: this.host.getTabId()!,
        toLeaderTabId: this.host.getCurrentLeaderTabId(),
        term: this.host.getCurrentLeaderTerm(),
      });

      const settled = await Promise.race([
        context.completion.promise.then(
          () => true,
          () => true,
        ),
        sleep(STORAGE_RESET_REQUEST_RETRY_MS).then(() => false),
      ]);
      if (settled) {
        await context.completion.promise;
        return;
      }

      if (Date.now() >= deadline) {
        const error = new Error(
          "Timed out waiting for the leader tab to begin browser storage reset.",
        );
        this.rejectContext(context, error);
        throw error;
      }
    }

    await context.completion.promise;
  }

  handleSyncChannelMessage(message: TabSyncMessage): boolean {
    switch (message.type) {
      case "storage-reset-request":
        this.handleRequest(message);
        return true;
      case "storage-reset-begin":
        this.handleBegin(message);
        return true;
      case "storage-reset-ack":
        this.handleAck(message);
        return true;
      case "storage-reset-finished":
        this.handleFinished(message);
        return true;
      default:
        return false;
    }
  }

  private getOrCreateContext(requestId: string, initiatedBySelf: boolean): StorageResetContext {
    if (this.active?.requestId === requestId) {
      if (initiatedBySelf) {
        this.active.initiatedBySelf = true;
      }
      return this.active;
    }

    const completion = createDeferred<void>();
    void completion.promise.catch(() => undefined);

    const context: StorageResetContext = {
      requestId,
      initiatedBySelf,
      coordinatorTabId: null,
      begun: false,
      completed: false,
      preparePromise: null,
      completion,
    };
    this.active = context;
    return context;
  }

  private clearContext(requestId: string): void {
    if (this.active?.requestId === requestId) {
      this.active = null;
    }
    if (this.coordinator?.requestId === requestId) {
      this.coordinator = null;
    }
  }

  private resolveContext(context: StorageResetContext): void {
    if (context.completed) return;
    context.completed = true;
    context.completion.resolve();
    this.clearContext(context.requestId);
  }

  private rejectContext(context: StorageResetContext, error: unknown): void {
    if (context.completed) return;
    context.completed = true;
    context.completion.reject(toError(error, "Browser storage reset failed"));
    this.clearContext(context.requestId);
  }

  private async prepare(context: StorageResetContext, coordinatorTabId: string): Promise<string> {
    if (context.preparePromise) {
      return await context.preparePromise;
    }

    context.begun = true;
    context.coordinatorTabId = coordinatorTabId;
    context.preparePromise = (async () => {
      await this.host.ensureBridgeReady();

      const namespace = this.host.getCurrentWorkerNamespace();
      await this.host.shutdownWorkerAndClients();

      const tabId = this.host.getTabId();
      if (tabId && coordinatorTabId !== tabId) {
        this.host.postSyncChannelMessage({
          type: "storage-reset-ack",
          requestId: context.requestId,
          fromTabId: tabId,
          namespace,
        });
      }

      return namespace;
    })();

    return await context.preparePromise;
  }

  private async waitForQuiescence(coordinator: CoordinatorState): Promise<void> {
    while (true) {
      const now = Date.now();
      const elapsed = now - coordinator.startedAtMs;
      const idleMs = now - coordinator.lastAckAtMs;
      if (elapsed >= STORAGE_RESET_DISCOVERY_WINDOW_MS && idleMs >= STORAGE_RESET_ACK_QUIET_MS) {
        return;
      }
      await sleep(25);
    }
  }

  private async collectNamespaces(extraNamespaces: Iterable<string>): Promise<string[]> {
    const namespaces = new Set<string>();
    const primaryDbName = this.host.getPrimaryDbName();
    if (primaryDbName) {
      namespaces.add(primaryDbName);
    }
    for (const namespace of extraNamespaces) {
      namespaces.add(namespace);
    }

    if (!primaryDbName) {
      return [...namespaces];
    }

    const rootDirectory = await navigator.storage.getDirectory();
    const rootWithEntries = rootDirectory as FileSystemDirectoryHandle & {
      entries?: () => AsyncIterable<[string, FileSystemHandle]>;
    };
    if (typeof rootWithEntries.entries !== "function") {
      return [...namespaces];
    }

    const suffix = ".opfsbtree";
    const fallbackPrefix = `${primaryDbName}__fallback__`;

    for await (const [name] of rootWithEntries.entries()) {
      if (!name.endsWith(suffix)) continue;
      const namespace = name.slice(0, -suffix.length);
      if (namespace === primaryDbName || namespace.startsWith(fallbackPrefix)) {
        namespaces.add(namespace);
      }
    }

    return [...namespaces];
  }

  private async removeNamespaceFile(namespace: string): Promise<void> {
    const rootDirectory = await navigator.storage.getDirectory();
    const fileName = `${namespace}.opfsbtree`;
    try {
      await rootDirectory.removeEntry(fileName, { recursive: false });
    } catch (error) {
      const name = (error as { name?: string } | undefined)?.name;
      if (name === "NotFoundError") {
        return;
      }
      if (name === "NoModificationAllowedError" || name === "InvalidStateError") {
        throw new Error(
          `Failed to delete browser storage for "${namespace}" because OPFS is locked by another tab. Close other tabs and retry.`,
        );
      }
      throw new Error(
        `Failed to delete browser storage for "${namespace}": ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  private async runSingleTab(context: StorageResetContext): Promise<void> {
    const coordinatorTabId = this.host.getTabId() ?? "single-tab-reset";
    let resultError: Error | null = null;

    try {
      const namespace = await this.prepare(context, coordinatorTabId);
      const namespaces = await this.collectNamespaces([namespace]);
      for (const candidate of namespaces) {
        await this.removeNamespaceFile(candidate);
      }
    } catch (error) {
      resultError = toError(error, "Browser storage reset failed");
    }

    try {
      await this.host.resumeWorker();
    } catch (error) {
      if (!resultError) {
        resultError = toError(error, "Failed to restart browser worker after storage reset");
      }
    }

    if (resultError) {
      throw resultError;
    }
  }

  private async startAsCoordinator(context: StorageResetContext): Promise<void> {
    if (this.coordinator?.requestId === context.requestId) {
      return await (this.coordinator.runPromise ?? context.completion.promise);
    }

    const tabId = this.host.getTabId();
    if (!tabId || this.host.getTabRole() !== "leader") {
      throw new Error("Storage reset coordination requires the current tab to be the leader.");
    }

    const coordinator: CoordinatorState = {
      requestId: context.requestId,
      startedAtMs: Date.now(),
      lastAckAtMs: Date.now(),
      ackedNamespacesByTabId: new Map(),
      runPromise: null,
    };
    this.coordinator = coordinator;

    coordinator.runPromise = (async () => {
      let resultError: Error | null = null;

      try {
        this.host.postSyncChannelMessage({
          type: "storage-reset-begin",
          requestId: context.requestId,
          coordinatorTabId: tabId,
          term: this.host.getCurrentLeaderTerm(),
        });

        const localNamespace = await this.prepare(context, tabId);
        coordinator.ackedNamespacesByTabId.set(tabId, localNamespace);
        coordinator.lastAckAtMs = Date.now();

        await this.waitForQuiescence(coordinator);

        const namespaces = await this.collectNamespaces(
          coordinator.ackedNamespacesByTabId.values(),
        );
        for (const namespace of namespaces) {
          await this.removeNamespaceFile(namespace);
        }
      } catch (error) {
        resultError = toError(error, "Browser storage reset failed");
      }

      try {
        await this.host.resumeWorker();
      } catch (error) {
        if (!resultError) {
          resultError = toError(error, "Failed to restart browser worker after storage reset");
        }
      }

      this.host.postSyncChannelMessage({
        type: "storage-reset-finished",
        requestId: context.requestId,
        success: resultError === null,
        ...(resultError ? { errorMessage: resultError.message } : {}),
      });

      if (resultError) {
        throw resultError;
      }
    })()
      .then(() => {
        this.resolveContext(context);
      })
      .catch((error) => {
        this.rejectContext(context, error);
      })
      .finally(() => {
        if (this.coordinator?.requestId === context.requestId) {
          this.coordinator = null;
        }
      });

    await coordinator.runPromise;
  }

  private handleRequest(message: StorageResetRequestMessage): void {
    if (this.host.getTabRole() !== "leader") return;
    const tabId = this.host.getTabId();
    if (!tabId) return;
    if (message.fromTabId === tabId) return;
    if (message.toLeaderTabId && message.toLeaderTabId !== tabId) return;
    if (message.term !== this.host.getCurrentLeaderTerm()) return;
    if (this.active && this.active.requestId !== message.requestId) return;

    const context = this.getOrCreateContext(message.requestId, false);
    void this.startAsCoordinator(context).catch(() => undefined);
  }

  private handleBegin(message: StorageResetBeginMessage): void {
    const leaderTabId = this.host.getCurrentLeaderTabId();
    if (!leaderTabId) return;
    if (message.coordinatorTabId !== leaderTabId) return;
    if (message.term !== this.host.getCurrentLeaderTerm()) return;
    if (message.coordinatorTabId === this.host.getTabId()) return;
    if (this.active && this.active.requestId !== message.requestId) return;

    const context = this.getOrCreateContext(message.requestId, false);
    context.begun = true;
    context.coordinatorTabId = message.coordinatorTabId;

    void this.prepare(context, message.coordinatorTabId).catch((error) => {
      this.rejectContext(context, error);
    });
  }

  private handleAck(message: StorageResetAckMessage): void {
    const coordinator = this.coordinator;
    if (!coordinator || coordinator.requestId !== message.requestId) return;

    coordinator.ackedNamespacesByTabId.set(message.fromTabId, message.namespace);
    coordinator.lastAckAtMs = Date.now();
  }

  private handleFinished(message: StorageResetFinishedMessage): void {
    const context = this.active;
    if (!context || context.requestId !== message.requestId || context.completed) return;

    void (async () => {
      let resultError: Error | null = message.success
        ? null
        : new Error(message.errorMessage ?? "Browser storage reset failed");

      try {
        await this.host.resumeWorker();
      } catch (error) {
        if (!resultError) {
          resultError = toError(error, "Failed to restart browser worker after storage reset");
        }
      }

      if (resultError) {
        this.rejectContext(context, resultError);
      } else {
        this.resolveContext(context);
      }
    })();
  }
}

import type { BrowserInitializationWaitIntent } from "../runtime/native-runtime/browser-worker-protocol.js";
import type { OpenTransactionId, TxId } from "../runtime/client.js";
import type { TransactionReadCapability } from "../runtime/accepted-read.js";
import type { InitializationSeal, ReservedTxId } from "../runtime/provisional-initialization.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";

/** Minted inside the worker only after its real peer/session admission. */
export interface DurableInitializationAdmission {
  readonly runtime: NativeRuntimeAdapter;
  readonly context: string | null | undefined;
  readonly readCapability: TransactionReadCapability;
  readonly claims: Record<string, unknown>;
  assertCurrent(): void;
}

type Phase = "opening" | "open" | "sealing" | "sealed" | "publishing" | "retired";
type Slot = {
  owner: object;
  admission: DurableInitializationAdmission;
  id: OpenTransactionId;
  phase: Phase;
  accepting: boolean;
  revoked: boolean;
  abort: AbortController;
  tail: Promise<void>;
  failed: boolean;
  failure?: unknown;
  cleanupFailed: boolean;
  cleanupFailure?: unknown;
  seal?: InitializationSeal;
};

/**
 * Keeps the original batch and seal in their actual durable worker. Handles
 * select this private registry; neither a native seal nor a foreground batch
 * is transferred or adopted. The transport must validate requests separately.
 */
export class DurableInitializationOwner {
  private readonly slots = new Map<string, Slot>();
  private closed = false;
  private readonly published = new Map<
    TxId,
    {
      owner: object;
      runtime: NativeRuntimeAdapter;
      waitIds: Set<string>;
    }
  >();
  // Terminal results belong to an existing published write, not active wait
  // capacity. They prevent late replies/cancellations from resurrecting waits.
  private readonly publishedWaits = new Map<
    string,
    {
      owner: object;
      tx: TxId;
      tier: "local" | "global";
      promise: Promise<void>;
    }
  >();
  private readonly waits = new Map<
    string,
    {
      owner: object;
      tx: TxId;
      tier: "local" | "global";
      abort: AbortController;
      promise: Promise<void>;
    }
  >();
  private readonly cancelledWaits = new Map<string, object>();
  private readonly reservedWaits = new Map<string, object>();

  async begin(
    owner: object,
    admission: DurableInitializationAdmission,
    id: OpenTransactionId,
  ): Promise<string> {
    if (this.closed || this.slots.size >= 64)
      throw new Error("Durable initialization owner is closed or has 64 active transactions");
    admission.assertCurrent();
    const handle = crypto.randomUUID();
    const slot: Slot = {
      owner,
      admission: {
        runtime: admission.runtime,
        context: admission.context,
        readCapability: admission.readCapability,
        claims: structuredClone(admission.claims),
        assertCurrent: admission.assertCurrent.bind(admission),
      },
      id,
      phase: "opening",
      accepting: false,
      revoked: false,
      abort: new AbortController(),
      tail: Promise.resolve(),
      failed: false,
      cleanupFailed: false,
    };
    this.slots.set(handle, slot);
    const opening = (async () => {
      let opened = false;
      try {
        await slot.admission.runtime.openInitializationTransaction(
          id,
          slot.admission.context,
          slot.admission.readCapability,
          slot.admission.claims,
          () => this.assertCurrent(handle, slot),
        );
        opened = true;
        this.assertCurrent(handle, slot);
        slot.phase = "open";
        slot.accepting = true;
      } catch (error) {
        if (opened) {
          try {
            await slot.admission.runtime.rollbackTransaction(id);
          } catch (cleanup) {
            throw new AggregateError([error, cleanup], "Initialization opening cleanup failed");
          }
        }
        throw error;
      }
    })();
    slot.tail = opening.then(
      () => {},
      (error) => {
        slot.failed = true;
        slot.failure = error;
        if (error instanceof AggregateError) {
          slot.cleanupFailed = true;
          slot.cleanupFailure = error;
        }
      },
    );
    try {
      await opening;
      return handle;
    } catch (error) {
      this.slots.delete(handle);
      throw error;
    }
  }

  /** Called only by fixed worker-side operations, never by a wire callback. */
  run<T>(
    owner: object,
    handle: string,
    operation: (
      runtime: NativeRuntimeAdapter,
      id: OpenTransactionId,
      context: string | null | undefined,
      signal: AbortSignal,
    ) => T | Promise<T>,
  ): Promise<T> {
    const slot = this.get(owner, handle);
    if (!slot.accepting) throw new Error("Initialization transaction no longer accepts operations");
    const result = slot.tail.then(async () => {
      this.assertCurrent(handle, slot);
      if (slot.failed) throw slot.failure;
      if (slot.phase !== "open") throw new Error("Initialization transaction is not open");
      const value = await operation(
        slot.admission.runtime,
        slot.id,
        slot.admission.context,
        slot.abort.signal,
      );
      this.assertCurrent(handle, slot);
      return value;
    });
    slot.tail = result.then(
      () => {},
      (error) => {
        slot.failed = true;
        slot.failure = error;
        slot.accepting = false;
      },
    );
    return result;
  }

  seal(owner: object, handle: string): Promise<ReservedTxId> {
    const slot = this.get(owner, handle);
    if (!slot.accepting) throw new Error("Initialization seal already requested");
    slot.accepting = false;
    const result = slot.tail.then(async () => {
      this.assertCurrent(handle, slot);
      if (slot.failed) throw slot.failure;
      slot.phase = "sealing";
      const seal = await slot.admission.runtime.sealInitializationTransaction(slot.id);
      slot.seal = seal;
      try {
        this.assertCurrent(handle, slot);
      } catch (error) {
        try {
          await slot.admission.runtime.cancelInitializationTransaction(seal);
        } catch (cleanup) {
          slot.cleanupFailed = true;
          slot.cleanupFailure = cleanup;
          throw new AggregateError([error, cleanup], "Initialization retirement cleanup failed");
        } finally {
          slot.seal = undefined;
          slot.phase = "retired";
          this.slots.delete(handle);
        }
        throw error;
      }
      slot.phase = "sealed";
      return seal.reservedTxId;
    });
    slot.tail = result.then(
      () => {},
      (error) => {
        slot.failed = true;
        slot.failure = error;
      },
    );
    return result;
  }

  commit(
    owner: object,
    handle: string,
    waitIntents: BrowserInitializationWaitIntent[] = [],
  ): Promise<TxId> {
    this.validateWaitIntents(waitIntents);
    const slot = this.get(owner, handle);
    if (!slot.accepting) throw new Error("Initialization transaction no longer accepts commit");
    this.reserveWaitIntents(owner, waitIntents);
    slot.accepting = false;
    const result = slot.tail
      .then(async () => {
        let committed = false;
        try {
          this.assertCurrent(handle, slot);
          if (slot.failed) throw slot.failure;
          slot.phase = "publishing";
          const tx = await slot.admission.runtime.commitTransaction(slot.id, (tx) =>
            this.recordPublished(owner, slot, tx, waitIntents),
          );
          committed = true;
          this.assertCurrent(handle, slot);
          return tx;
        } catch (error) {
          if (!committed && !slot.revoked) {
            try {
              await slot.admission.runtime.rollbackTransaction(slot.id);
            } catch (cleanup) {
              throw new AggregateError([error, cleanup], "Durable commit rollback failed");
            }
          }
          throw error;
        }
      })
      .finally(() => {
        for (const intent of waitIntents) this.reservedWaits.delete(intent.waitId);
        slot.phase = "retired";
        this.slots.delete(handle);
      });
    slot.tail = result.then(
      () => {},
      (error) => {
        slot.failed = true;
        slot.failure = error;
      },
    );
    return result;
  }

  publish(
    owner: object,
    handle: string,
    waitIntents: BrowserInitializationWaitIntent[] = [],
  ): Promise<TxId> {
    this.validateWaitIntents(waitIntents);
    const slot = this.get(owner, handle);
    this.assertCurrent(handle, slot);
    if (slot.phase !== "sealed" || !slot.seal)
      throw new Error("Initialization transaction is not sealed");
    this.reserveWaitIntents(owner, waitIntents);
    slot.phase = "publishing";
    const seal = slot.seal;
    slot.seal = undefined;
    const result = (async () => {
      try {
        const tx = await slot.admission.runtime.publishInitializationTransaction(seal, (tx) =>
          this.recordPublished(owner, slot, tx, waitIntents),
        );
        this.assertCurrent(handle, slot);
        return tx;
      } finally {
        for (const intent of waitIntents) this.reservedWaits.delete(intent.waitId);
        slot.phase = "retired";
        this.slots.delete(handle);
      }
    })();
    slot.tail = result.then(
      () => {},
      (error) => {
        slot.failed = true;
        slot.failure = error;
      },
    );
    return result;
  }

  wait(owner: object, tx: TxId, tier: "local" | "global", waitId: string): Promise<void> {
    this.validateWaitIntents([{ waitId, tier }]);
    const write = this.published.get(tx);
    if (
      this.closed ||
      !write ||
      write.owner !== owner ||
      (this.reservedWaits.has(waitId) && this.reservedWaits.get(waitId) !== owner)
    )
      throw new Error("Durable wait does not belong to this live peer");
    return this.startWait(owner, write.runtime, tx, tier, waitId).promise;
  }

  cancelWait(owner: object, waitId: string): void {
    this.validateWaitIntents([{ waitId, tier: "local" }]);
    const known = this.publishedWaits.get(waitId);
    if (
      (known && known.owner !== owner) ||
      (this.reservedWaits.has(waitId) && this.reservedWaits.get(waitId) !== owner)
    )
      throw new Error("Cannot cancel another peer's durability wait");
    if (known) {
      const waiting = this.waits.get(waitId);
      waiting?.abort.abort(new Error("Durable write wait ended"));
      this.waits.delete(waitId);
      // A completed result is already terminal. A late cancellation does not
      // create a global tombstone or change its write's actual fate.
      return;
    }
    const previous = this.cancelledWaits.get(waitId);
    if (previous && previous !== owner) throw new Error("Durable wait cancellation owner differs");
    if (previous) return;
    if (this.cancelledWaits.size >= 4096)
      throw new Error("Too many outstanding durable wait cancellations");
    this.cancelledWaits.set(waitId, owner);
  }

  private validateWaitIntents(intents: BrowserInitializationWaitIntent[]): void {
    if (
      !Array.isArray(intents) ||
      intents.length > 64 ||
      new Set(intents.map((i) => i.waitId)).size !== intents.length
    )
      throw new Error("Invalid durable wait intent list");
    for (const intent of intents) {
      if (
        !intent ||
        typeof intent.waitId !== "string" ||
        !/^[0-9a-f-]{36}$/.test(intent.waitId) ||
        (intent.tier !== "local" && intent.tier !== "global")
      )
        throw new Error("Invalid durable wait intent");
      if (this.waits.has(intent.waitId)) {
        // Reusing an existing waiter is checked against owner/TxId by wait().
        continue;
      }
    }
  }

  private reserveWaitIntents(owner: object, intents: BrowserInitializationWaitIntent[]): void {
    if (this.waits.size + this.reservedWaits.size + intents.length > 4096)
      throw new Error("Too many outstanding durable wait intents");
    for (const { waitId } of intents) {
      if (
        this.publishedWaits.has(waitId) ||
        this.reservedWaits.has(waitId) ||
        (this.cancelledWaits.has(waitId) && this.cancelledWaits.get(waitId) !== owner)
      )
        throw new Error("Durable wait intent is already in use");
    }
    for (const { waitId } of intents) this.reservedWaits.set(waitId, owner);
  }

  private recordPublished(
    owner: object,
    slot: Slot,
    tx: TxId,
    intents: BrowserInitializationWaitIntent[],
  ): void {
    this.published.set(tx, { owner, runtime: slot.admission.runtime, waitIds: new Set() });
    for (const { waitId, tier } of intents)
      this.startWait(owner, slot.admission.runtime, tx, tier, waitId);
  }

  private startWait(
    owner: object,
    runtime: NativeRuntimeAdapter,
    tx: TxId,
    tier: "local" | "global",
    waitId: string,
  ) {
    const previous = this.publishedWaits.get(waitId);
    if (previous) {
      if (previous.owner !== owner || previous.tx !== tx || previous.tier !== tier)
        throw new Error("Durable wait identity is already in use");
      return previous;
    }
    const write = this.published.get(tx);
    if (!write || write.owner !== owner) throw new Error("Durable wait lost its publication owner");
    if (write.waitIds.size >= 4096)
      throw new Error("Published write reached its retained wait identity limit");
    const abort = new AbortController();
    const cancelled = this.cancelledWaits.get(waitId);
    if (cancelled && cancelled !== owner)
      throw new Error("Durable wait cancellation owner differs");
    if (cancelled) {
      this.cancelledWaits.delete(waitId);
      abort.abort(new Error("Durable write wait ended"));
    }
    if (!abort.signal.aborted && this.waits.size >= 4096)
      throw new Error("Too many outstanding durable waits");
    write.waitIds.add(waitId);
    // Register a LIVE native wait synchronously before host progress. An
    // already-ended intent must not claim a later mutation-error fallback.
    let promise: Promise<void>;
    try {
      promise = abort.signal.aborted
        ? Promise.reject(abort.signal.reason)
        : runtime.waitForTransaction(tx, tier, { signal: abort.signal });
    } catch (error) {
      promise = Promise.reject(error);
    }
    const waiting = { owner, tx, tier, abort, promise };
    this.publishedWaits.set(waitId, waiting);
    if (!abort.signal.aborted) this.waits.set(waitId, waiting);
    const release = () => {
      if (this.waits.get(waitId) === waiting) this.waits.delete(waitId);
    };
    void promise.then(release, release);
    return waiting;
  }

  async cancel(owner: object, handle: string): Promise<void> {
    const slot = this.get(owner, handle);
    if (slot.phase === "publishing")
      throw new Error(
        "Initialization publication already started; recover its exact journal status",
      );
    slot.revoked = true;
    slot.accepting = false;
    slot.abort.abort(new Error("Initialization transaction cancelled"));
    try {
      if (slot.phase === "opening" || slot.phase === "sealing") {
        await slot.tail;
        if (slot.cleanupFailed) throw slot.cleanupFailure;
      } else if (slot.seal) {
        const seal = slot.seal;
        slot.seal = undefined;
        await slot.admission.runtime.cancelInitializationTransaction(seal);
      } else if (slot.phase !== "retired") {
        // Abort native reads and roll back now; waiting for a read before its
        // cancellation/rollback would deadlock on that same pending read.
        await slot.admission.runtime.rollbackTransaction(slot.id);
        await slot.tail;
      }
    } finally {
      slot.phase = "retired";
      this.slots.delete(handle);
    }
  }

  async revokeOwner(owner: object): Promise<void> {
    for (const [tx, write] of this.published)
      if (write.owner === owner) {
        for (const id of write.waitIds) this.publishedWaits.delete(id);
        this.published.delete(tx);
      }
    for (const [id, waiting] of this.waits)
      if (waiting.owner === owner) {
        waiting.abort.abort(new Error("Durable wait peer retired"));
        this.waits.delete(id);
      }
    for (const [id, peer] of this.cancelledWaits)
      if (peer === owner) this.cancelledWaits.delete(id);
    for (const [id, peer] of this.reservedWaits) if (peer === owner) this.reservedWaits.delete(id);
    const owned = [...this.slots].filter(([, slot]) => slot.owner === owner);
    // Revoke all handles synchronously before any cleanup can yield.
    for (const [, slot] of owned) {
      slot.revoked = true;
      slot.accepting = false;
      slot.abort.abort(new Error("Initialization peer retired"));
    }
    const results = await Promise.allSettled(
      owned.map(async ([handle, slot]) => {
        if (slot.phase === "publishing") {
          await slot.tail;
          return;
        }
        if (this.slots.get(handle) === slot) await this.cancel(owner, handle);
      }),
    );
    const failed = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed.length)
      throw new AggregateError(
        failed.map((r) => r.reason),
        "Initialization owner cleanup failed",
      );
  }

  /** Paired only with actual native owner discard/storage invalidation. */
  discard(): void {
    this.closed = true;
    for (const waiting of this.waits.values())
      waiting.abort.abort(new Error("Durable storage owner discarded"));
    this.waits.clear();
    this.published.clear();
    this.publishedWaits.clear();
    this.cancelledWaits.clear();
    this.reservedWaits.clear();
    for (const slot of this.slots.values()) {
      slot.revoked = true;
      slot.accepting = false;
      slot.abort.abort(new Error("Initialization storage owner discarded"));
    }
    this.slots.clear();
  }

  async close(): Promise<void> {
    this.closed = true;
    const owners = new Set(
      [...this.slots.values(), ...this.published.values(), ...this.waits.values()].map(
        (slot) => slot.owner,
      ),
    );
    const results = await Promise.allSettled([...owners].map((owner) => this.revokeOwner(owner)));
    const failed = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed.length)
      throw new AggregateError(
        failed.map((r) => r.reason),
        "Initialization host cleanup failed",
      );
  }

  private get(owner: object, handle: string): Slot {
    const slot = this.slots.get(handle);
    if (!slot || slot.owner !== owner)
      throw new Error("Initialization handle is foreign or retired");
    return slot;
  }
  private assertCurrent(handle: string, slot: Slot): void {
    if (this.closed || slot.revoked || this.slots.get(handle) !== slot)
      throw new Error("Initialization owner lifetime ended");
    slot.admission.assertCurrent();
    slot.admission.runtime.assertInitializationOwnerContext(
      slot.admission.context,
      slot.admission.claims,
    );
  }
}

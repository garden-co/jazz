/**
 * Opt-in diagnostic instrumentation for the WorkerBridge / supervisor /
 * subscription pipeline.
 *
 * The leader-tab topology splits state across three runtimes (main-thread
 * cache, leader-tab dedicated worker, optional follower-port bridge) and a
 * SharedWorker broker. When a subscription on the main thread silently
 * stops receiving deltas — but the leader's worker still has the data
 * (visible in the Inspector) — the most likely break is that
 * `WorkerBridge.installForwarderInternal` wasn't called on the post-
 * migration bridge, so the worker has nowhere to echo deltas to.
 *
 * Enable with `globalThis.__JAZZ_DEBUG_FORWARDER__ = true` in the
 * browser console *before* you reproduce. All instrumented events get
 * prefixed `[jazz-debug]` console logs and the per-Db counters are
 * available via `db.jazzDebugDump()`.
 *
 * When the flag is off, every helper here is a constant-time no-op —
 * production paths never see strings or allocations.
 */

const DEBUG_FLAG = "__JAZZ_DEBUG_FORWARDER__" as const;

export function jazzDebugEnabled(): boolean {
  return (globalThis as Record<string, unknown>)[DEBUG_FLAG] === true;
}

export function jazzDebugLog(...args: unknown[]): void {
  if (!jazzDebugEnabled()) return;
  // eslint-disable-next-line no-console
  console.log("[jazz-debug]", ...args);
}

export interface JazzDebugCounters {
  bridgesCreated: number;
  bridgeInitsStarted: number;
  bridgeInitsResolved: number;
  bridgeInitsRejected: number;
  bridgesShutdown: number;
  bridgesMigrated: number;
  forwardersSetPending: number;
  forwardersSetCleared: number;
  forwardersInstalled: number;
  dbAttachWorkerBridge: number;
  dbSupervisorStateChanges: number;
  dbSubscriptionsCreated: number;
  dbSubscriptionDeltasObserved: number;
}

export function emptyJazzDebugCounters(): JazzDebugCounters {
  return {
    bridgesCreated: 0,
    bridgeInitsStarted: 0,
    bridgeInitsResolved: 0,
    bridgeInitsRejected: 0,
    bridgesShutdown: 0,
    bridgesMigrated: 0,
    forwardersSetPending: 0,
    forwardersSetCleared: 0,
    forwardersInstalled: 0,
    dbAttachWorkerBridge: 0,
    dbSupervisorStateChanges: 0,
    dbSubscriptionsCreated: 0,
    dbSubscriptionDeltasObserved: 0,
  };
}

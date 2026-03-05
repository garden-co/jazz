import type { useDb } from "jazz-tools/react";
import type { FuelDeposit, PlayerInit } from "../../schema/app";
import { app } from "../../schema/app";
import type { FuelType } from "../game/constants";
import { DB_SYNC_INTERVAL_MS, FUEL_TYPES, MOON_SURFACE_WIDTH } from "../game/constants";
import { seededRand } from "../game/world.js";

/** Base number of uncollected deposits per fuel type. */
export const DEPOSITS_PER_TYPE = 3;

// ---------------------------------------------------------------------------
// SyncInputs — subscription data pushed from React each render
// ---------------------------------------------------------------------------

export interface SyncInputs {
  settled: boolean;
  /** True once the local player-row subscription has returned (may be before edge deposits settle). */
  localPlayerSettled: boolean;
  uncollectedDeposits: FuelDeposit[];
  myCollectedDeposits: FuelDeposit[];
  allDepositsRaw: FuelDeposit[];
  localPlayerRows: Array<{ id: string }>;
  perTypeLimits: number[];
  // Debug-only (passed through to DebugPanel)
  perTypeCounts: number[];
  myCollectedCount: number;
  debugTotalDeposits: number;
}

// ---------------------------------------------------------------------------
// SyncManager — owns all DB write state, replaces useSyncLoop + writes.ts
// ---------------------------------------------------------------------------

export class SyncManager {
  // Pending event queues
  private pendingCollections: string[] = [];
  private pendingRefuels: FuelType[] = [];
  private pendingShares: Array<{ fuelType: string; receiverPlayerId: string }> = [];
  private pendingBursts: string[] = [];
  private pendingMessages: string[] = [];
  // Retry queue: burst/refuel releases where the deposit wasn't yet in
  // allDepositsRaw (OPFS WHERE ENTRY race). Retried on the next flush cycle.
  private pendingRetryReleases: string[] = [];

  // Local map: deposit id → fuelType for deposits collected by this player that
  // may not yet appear in allDepositsRaw (WHERE ENTRY for local boolean writes
  // can lag).  Populated eagerly in collectDeposit() while the row is still in
  // uncollectedDeposits; used as fallback in releaseDeposit() and shareFuel().
  private collectedByThis = new Map<string, { fuelType: string; positionX: number }>(); // id → {fuelType, positionX}

  // Player sync state
  private dbRowId: string | null = null;
  private lastSynced: PlayerInit | null = null;
  latestState: PlayerInit | null = null;

  // Reconciliation
  private hasReconciled = false;

  // Teardown flag — set by destroy() to suppress flush errors after cleanup
  private destroyed = false;

  // Prevent concurrent flush calls from racing on dbRowId / inserts
  private flushing = false;

  // Latest subscription data (pushed from React each render)
  inputs: SyncInputs = {
    settled: false,
    localPlayerSettled: false,
    uncollectedDeposits: [],
    myCollectedDeposits: [],
    allDepositsRaw: [],
    localPlayerRows: [],
    perTypeLimits: [],
    perTypeCounts: [],
    myCollectedCount: 0,
    debugTotalDeposits: 0,
  };

  private intervalId: ReturnType<typeof setInterval>;

  constructor(
    private db: ReturnType<typeof useDb>,
    private playerId: string,
  ) {
    this.intervalId = setInterval(() => {
      this.flush().catch((e) => {
        if (!this.destroyed) console.error("SyncManager flush error:", e);
      });
    }, DB_SYNC_INTERVAL_MS);
  }

  // --- Public API (called from game callbacks) ---

  collectDeposit(id: string): void {
    this.pendingCollections.push(id);
    // Record fuelType+positionX now while the deposit is still in allDepositsRaw (uncollected).
    // Provides a fallback for releaseDeposit (DELETE+INSERT) if WHERE ENTRY hasn't fired yet.
    const dep = this.inputs.allDepositsRaw.find((d) => d.id === id);
    if (dep) {
      this.collectedByThis.set(id, { fuelType: dep.fuelType, positionX: dep.positionX });
      console.log("[SyncManager] collectDeposit: tracked", id, dep.fuelType);
    } else {
      console.warn(
        "[SyncManager] collectDeposit: deposit NOT in allDepositsRaw",
        id,
        "allDepositsRaw.length=",
        this.inputs.allDepositsRaw.length,
      );
    }
  }

  refuel(fuelType: FuelType): void {
    this.pendingRefuels.push(fuelType);
  }

  shareFuel(fuelType: string, receiverPlayerId: string): void {
    this.pendingShares.push({ fuelType, receiverPlayerId });
  }

  burstDeposit(fuelType: string): void {
    this.pendingBursts.push(fuelType);
  }

  sendMessage(text: string): void {
    this.pendingMessages.push(text);
  }

  updateState(state: PlayerInit): void {
    this.latestState = state;
  }

  setInputs(inputs: SyncInputs): void {
    this.inputs = inputs;
  }

  destroy(): void {
    this.destroyed = true;
    clearInterval(this.intervalId);
  }

  // --- Private ---

  private async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      await this.doFlush();
    } finally {
      this.flushing = false;
    }
  }

  private async doFlush(): Promise<void> {
    const ds = this.inputs;

    // Reconcile deposits once after edge subscriptions settle
    if (ds.settled && !this.hasReconciled) {
      this.hasReconciled = true;
      await reconcileDeposits(this.db, ds.uncollectedDeposits, ds.perTypeLimits);
    }

    // Flush deposit collections (edge tier — broadcasts to remote clients'
    // edge subscriptions, enabling cross-client sharing).  Worker-tier writes
    // only update local OPFS and do not trigger WHERE ENTRY on other clients'
    // where({ collected: true }) subscriptions.
    for (const depId of this.pendingCollections.splice(0)) {
      await this.db.update(
        app.fuel_deposits,
        depId,
        { collected: true, collectedBy: this.playerId },
        { tier: "edge" },
      );
    }

    // Flush deposit releases (refuel + burst) — reset so the deposit can be collected again.
    // Includes any retries from the previous flush cycle where the deposit wasn't yet
    // visible in allDepositsRaw (OPFS WHERE ENTRY can lag behind WHERE EXIT).
    const releasedIds = new Set<string>();
    const retryReleases: string[] = [];
    const toRelease = [
      ...this.pendingRetryReleases.splice(0),
      ...this.pendingRefuels.splice(0),
      ...this.pendingBursts.splice(0),
    ];
    if (toRelease.length > 0) {
      console.log(
        "[SyncManager] doFlush: releasing",
        toRelease,
        "allDepositsRaw.length=",
        this.inputs.allDepositsRaw.length,
        "collectedByThis=",
        [...this.collectedByThis.entries()].map(([id, v]) => `${id.slice(0, 8)}:${v.fuelType}`),
      );
    }
    for (const fuelType of toRelease) {
      const released = await this.releaseDeposit(fuelType, releasedIds);
      if (!released) retryReleases.push(fuelType);
    }
    if (retryReleases.length > 0) {
      console.warn("[SyncManager] doFlush: retry releases", retryReleases);
    }
    this.pendingRetryReleases.push(...retryReleases);

    // Flush fuel shares — reassign the deposit to the receiver.
    // The receiver already has the row in their where({ collected: true })
    // subscription (it entered when this player collected it). Updating
    // collectedBy propagates as a normal row update — no WHERE re-evaluation.
    for (const share of this.pendingShares.splice(0)) {
      // Primary: find via subscription data.
      let shareId: string | undefined = ds.allDepositsRaw.find(
        (d) =>
          d.collected &&
          d.collectedBy === this.playerId &&
          d.fuelType === share.fuelType &&
          !releasedIds.has(d.id),
      )?.id;
      // Fallback: use locally-tracked id if WHERE ENTRY hasn't fired yet.
      if (!shareId) {
        for (const [id, info] of this.collectedByThis) {
          if (info.fuelType === share.fuelType && !releasedIds.has(id)) {
            shareId = id;
            break;
          }
        }
      }
      if (shareId) {
        releasedIds.add(shareId);
        this.collectedByThis.delete(shareId);
        await this.db.update(
          app.fuel_deposits,
          shareId,
          { collectedBy: share.receiverPlayerId },
          { tier: "edge" },
        );
      }
    }

    // Flush chat messages
    for (const text of this.pendingMessages.splice(0)) {
      await this.db.insert(
        app.chat_messages,
        { playerId: this.playerId, message: text, createdAt: Math.floor(Date.now() / 1000) },
        { tier: "edge" },
      );
    }

    // Release stale collected deposits when the game is restarting.
    const state = this.latestState;
    const mode = state?.mode;
    if (mode === "start" || mode === "descending") {
      for (const d of ds.allDepositsRaw) {
        if (d.collected && d.collectedBy === this.playerId && !releasedIds.has(d.id)) {
          releasedIds.add(d.id);
          this.collectedByThis.delete(d.id);
          await this.db.update(app.fuel_deposits, d.id, { collected: false, collectedBy: "" });
        }
      }
      // Fallback: also release locally-tracked deposits not yet in allDepositsRaw.
      for (const [id] of this.collectedByThis.entries()) {
        if (!releasedIds.has(id)) {
          releasedIds.add(id);
          this.collectedByThis.delete(id);
          await this.db.update(
            app.fuel_deposits,
            id,
            { collected: false, collectedBy: "" },
            { tier: "edge" },
          );
        }
      }
    }

    // Sync player state (insert or update) — kept last so slow edge round-trips
    // do not block deposit operations earlier in the flush.
    if (state) {
      if (!this.dbRowId && ds.localPlayerRows.length > 0) {
        this.dbRowId = ds.localPlayerRows[0].id;
      }
      if (this.dbRowId) {
        if (!this.lastSynced || playerStateChanged(this.lastSynced, state)) {
          await this.db.update(app.players, this.dbRowId, state, { tier: "edge" });
          this.lastSynced = { ...state };
        }
      } else if (ds.settled) {
        this.dbRowId = await this.db.insert(app.players, state, { tier: "edge" });
        this.lastSynced = { ...state };
      }
    }
  }

  /**
   * Reset a collected deposit of the given fuel type (owned by this player) so
   * it can be collected again.  releasedIds guards against double-processing
   * within the same flush cycle.
   *
   * Uses DELETE+INSERT rather than UPDATE collected:true→false.  Worker-tier
   * INSERT fires WHERE ENTRY reliably for the where({collected:false}) worker
   * subscription.  UPDATE-based WHERE ENTRY (re-entry) does not fire because
   * useAll subscriptions default to worker tier regardless of the "edge" hint.
   *
   * Primary: look up via subscription data (allDepositsRaw).
   * Fallback: use collectedByThis map if WHERE ENTRY hasn't fired yet.
   */
  private async releaseDeposit(fuelType: string, releasedIds: Set<string>): Promise<boolean> {
    // Primary: subscription-based lookup.
    const dep = this.inputs.allDepositsRaw.find(
      (d) =>
        d.collected &&
        d.collectedBy === this.playerId &&
        d.fuelType === fuelType &&
        !releasedIds.has(d.id),
    );

    let depId: string | undefined;
    let positionX: number | undefined;

    if (dep) {
      depId = dep.id;
      positionX = dep.positionX;
    } else {
      // Fallback: eagerly-tracked map (safe with worker-tier collect writes).
      for (const [id, info] of this.collectedByThis) {
        if (info.fuelType === fuelType && !releasedIds.has(id)) {
          depId = id;
          positionX = info.positionX;
          break;
        }
      }
    }

    if (!depId || positionX === undefined) {
      console.warn(
        "[SyncManager] releaseDeposit: no deposit found for",
        fuelType,
        "allDepositsRaw.length=",
        this.inputs.allDepositsRaw.length,
        "collectedByThis.size=",
        this.collectedByThis.size,
      );
      return false;
    }
    releasedIds.add(depId);
    this.collectedByThis.delete(depId);
    // DELETE+INSERT: edge-tier INSERT fires WHERE ENTRY on the edge-subscribed
    // where({collected:false}) subscription.  Worker-tier INSERT does not
    // trigger WHERE ENTRY on edge subscriptions.  UPDATE-based WHERE ENTRY
    // (re-entry: collected:true → false) also does not fire reliably.
    console.log(
      "[SyncManager] releaseDeposit: DELETE+INSERT for",
      fuelType,
      depId,
      "positionX=",
      positionX,
    );
    await this.db.deleteFrom(app.fuel_deposits, depId);
    const newId = await this.db.insert(
      app.fuel_deposits,
      {
        fuelType,
        positionX,
        createdAt: Math.floor(Date.now() / 1000),
        collected: false,
        collectedBy: "",
      },
      { tier: "edge" },
    );
    console.log("[SyncManager] releaseDeposit: INSERT done, newId=", newId);
    return true;
  }
}

// ---------------------------------------------------------------------------
// Pure helpers (were in writes.ts, exported for tests)
// ---------------------------------------------------------------------------

/**
 * Reconcile deposits: insert missing deposits and trim excess per fuel type.
 *
 * Each player calls this once on join after the edge subscription settles.
 * Trimming excess handles the concurrent top-up race (two clients both
 * inserting a full set) and players leaving (reducing the target count).
 */
export async function reconcileDeposits(
  db: ReturnType<typeof useDb>,
  uncollectedDeposits: FuelDeposit[],
  perTypeLimits: number[],
) {
  const nowS = Math.floor(Date.now() / 1000);

  // Group uncollected deposits by fuel type
  const byType = new Map<string, FuelDeposit[]>();
  for (const ft of FUEL_TYPES) byType.set(ft, []);
  for (const d of uncollectedDeposits) {
    const arr = byType.get(d.fuelType);
    if (arr) arr.push(d);
  }

  for (let i = 0; i < FUEL_TYPES.length; i++) {
    const ft = FUEL_TYPES[i];
    const deposits = byType.get(ft) ?? [];
    const target = perTypeLimits[i];
    const diff = target - deposits.length;

    if (diff > 0) {
      // Insert missing deposits
      for (let j = 0; j < diff; j++) {
        // Use a deterministic seed so all players generate the same positions.
        // Seed: fuelTypeIndex * 1000 + slotIndex, where slotIndex = deposits.length + j
        // (the j-th missing slot starting from how many already exist).
        const slotSeed = i * 1000 + deposits.length + j;
        await db.insert(
          app.fuel_deposits,
          {
            fuelType: ft,
            positionX: Math.floor(seededRand(slotSeed) * MOON_SURFACE_WIDTH),
            createdAt: nowS,
            collected: false,
            collectedBy: "",
          },
          { tier: "edge" },
        );
      }
    } else if (diff < 0) {
      // Trim excess: remove the newest deposits first (highest createdAt)
      const sorted = [...deposits].sort((a, b) => b.createdAt - a.createdAt);
      for (let j = 0; j < -diff; j++) {
        await db.update(
          app.fuel_deposits,
          sorted[j].id,
          { collected: true, collectedBy: "__trimmed__" },
          { tier: "edge" },
        );
      }
    }
  }
}

/** Returns true if any synced field in PlayerInit has changed meaningfully. */
export function playerStateChanged(a: PlayerInit, b: PlayerInit): boolean {
  const POSITION_THRESHOLD = 2; // pixels
  const VELOCITY_THRESHOLD = 0.5; // pixels/tick
  return (
    a.mode !== b.mode ||
    Math.abs(a.positionX - b.positionX) > POSITION_THRESHOLD ||
    Math.abs(a.positionY - b.positionY) > POSITION_THRESHOLD ||
    Math.abs(a.velocityX - b.velocityX) > VELOCITY_THRESHOLD ||
    Math.abs(a.velocityY - b.velocityY) > VELOCITY_THRESHOLD ||
    a.landerFuelLevel !== b.landerFuelLevel ||
    a.landerSpawnX !== b.landerSpawnX ||
    a.name !== b.name ||
    a.color !== b.color ||
    a.requiredFuelType !== b.requiredFuelType ||
    a.thrusting !== b.thrusting
  );
}

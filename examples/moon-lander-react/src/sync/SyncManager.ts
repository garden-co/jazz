import type { useDb } from "jazz-tools/react";
import type { FuelDeposit, PlayerInit } from "../../schema/app";
import { app } from "../../schema/app";
import type { FuelType } from "../game/constants";
import { DB_SYNC_INTERVAL_MS, FUEL_TYPES, MOON_SURFACE_WIDTH } from "../game/constants";

/** Base number of uncollected deposits per fuel type. */
export const DEPOSITS_PER_TYPE = 3;

// ---------------------------------------------------------------------------
// SyncInputs — subscription data pushed from React each render
// ---------------------------------------------------------------------------

export interface SyncInputs {
  settled: boolean;
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

  // Player sync state
  private dbRowId: string | null = null;
  private lastSynced: PlayerInit | null = null;
  latestState: PlayerInit | null = null;

  // Reconciliation
  private hasReconciled = false;

  // Latest subscription data (pushed from React each render)
  inputs: SyncInputs = {
    settled: false,
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
    this.intervalId = setInterval(() => this.flush(), DB_SYNC_INTERVAL_MS);
  }

  // --- Public API (called from game callbacks) ---

  collectDeposit(id: string): void {
    this.pendingCollections.push(id);
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
    clearInterval(this.intervalId);
  }

  // --- Private ---

  private async flush(): Promise<void> {
    const ds = this.inputs;

    // Reconcile deposits once after edge subscriptions settle
    if (ds.settled && !this.hasReconciled) {
      this.hasReconciled = true;
      await reconcileDeposits(this.db, ds.uncollectedDeposits, ds.perTypeLimits);
    }

    // Sync player state (insert or update)
    const state = this.latestState;
    if (state) {
      if (!this.dbRowId && ds.localPlayerRows.length > 0) {
        this.dbRowId = ds.localPlayerRows[0].id;
      }
      if (this.dbRowId) {
        if (!this.lastSynced || playerStateChanged(this.lastSynced, state)) {
          await this.db.updatePersisted(app.players, this.dbRowId, state, "edge");
          this.lastSynced = { ...state };
        }
      } else if (ds.settled) {
        this.dbRowId = await this.db.insertPersisted(app.players, state, "edge");
        this.lastSynced = { ...state };
      }
    }

    // Flush deposit collections
    for (const depId of this.pendingCollections.splice(0)) {
      await this.db.updatePersisted(
        app.fuel_deposits,
        depId,
        { collected: true, collectedBy: this.playerId },
        "edge",
      );
    }

    // Flush deposit releases (refuel + burst) — same operation, merged
    for (const fuelType of this.pendingRefuels.splice(0)) {
      await this.releaseDeposit(fuelType);
    }
    for (const fuelType of this.pendingBursts.splice(0)) {
      await this.releaseDeposit(fuelType);
    }

    // Flush fuel shares
    for (const share of this.pendingShares.splice(0)) {
      const dep = ds.allDepositsRaw.find(
        (d) => d.collected && d.collectedBy === this.playerId && d.fuelType === share.fuelType,
      );
      if (dep) {
        await this.db.updatePersisted(
          app.fuel_deposits,
          dep.id,
          { collectedBy: share.receiverPlayerId },
          "edge",
        );
      }
    }

    // Flush chat messages
    for (const text of this.pendingMessages.splice(0)) {
      await this.db.insertPersisted(
        app.chat_messages,
        { playerId: this.playerId, message: text, createdAt: Math.floor(Date.now() / 1000) },
        "edge",
      );
    }

    // Release stale collected deposits when the game is restarting
    const mode = state?.mode;
    if (mode === "start" || mode === "descending") {
      for (const d of ds.allDepositsRaw) {
        if (d.collected && d.collectedBy === this.playerId) {
          await this.db.updatePersisted(
            app.fuel_deposits,
            d.id,
            { collected: false, collectedBy: "" },
            "edge",
          );
        }
      }
    }
  }

  /** Find a collected deposit of the given fuel type owned by this player and release it. */
  private async releaseDeposit(fuelType: string): Promise<void> {
    const dep = this.inputs.allDepositsRaw.find(
      (d) => d.collected && d.collectedBy === this.playerId && d.fuelType === fuelType,
    );
    if (dep) {
      await this.db.updatePersisted(
        app.fuel_deposits,
        dep.id,
        { collected: false, collectedBy: "" },
        "edge",
      );
    }
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
        await db.insertPersisted(
          app.fuel_deposits,
          {
            fuelType: ft,
            positionX: Math.floor(Math.random() * MOON_SURFACE_WIDTH),
            createdAt: nowS,
            collected: false,
            collectedBy: "",
          },
          "edge",
        );
      }
    } else if (diff < 0) {
      // Trim excess: remove the newest deposits first (highest createdAt)
      const sorted = [...deposits].sort((a, b) => b.createdAt - a.createdAt);
      for (let j = 0; j < -diff; j++) {
        await db.updatePersisted(
          app.fuel_deposits,
          sorted[j].id,
          { collected: true, collectedBy: "__trimmed__" },
          "edge",
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

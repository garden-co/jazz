/**
 * SyncManager — all Jazz writes for the game.
 *
 * Jazz write APIs used here:
 *   db.insertDurable(table, data, { tier })    — create a new row (fires WHERE ENTRY cross-client)
 *   db.updateDurable(table, id, data, { tier }) — update fields (fires WHERE EXIT/ENTRY cross-client)
 *   db.deleteDurable(table, id, { tier })  — delete a row (local only — server can't forward deleted objects)
 *
 * The "edge" tier broadcasts the write to all connected clients' live
 * subscriptions, triggering WHERE ENTRY / WHERE EXIT events remotely.
 * The default "worker" tier only updates the local OPFS database.
 *
 * Writes are fired immediately when game callbacks are invoked — no batching
 * interval. releasingIds guards against double-releasing the same deposit
 * across concurrent async operations.
 */

import type { useDb } from "jazz-tools/react";
import type { FuelDeposit, PlayerInit } from "../../schema/app";
import { app } from "../../schema/app";
import type { FuelType } from "../game/constants";
import { FUEL_TYPES, MOON_SURFACE_WIDTH } from "../game/constants";
import { seededRand } from "../game/world.js";

/** Base number of uncollected deposits per fuel type on the surface. */
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
// SyncManager
// ---------------------------------------------------------------------------

export class SyncManager {
  // Local map: deposit id → fuelType for deposits collected by this player that
  // may not yet appear in allDepositsRaw (WHERE ENTRY for local boolean writes
  // can lag).  Populated eagerly in collectDeposit() while the row is still in
  // uncollectedDeposits; used as fallback in releaseDeposit() and shareFuel().
  private collectedByThis = new Map<string, { fuelType: string; positionX: number }>();

  // Guards against double-releasing the same deposit across concurrent async ops
  private releasingIds = new Set<string>();

  // Player sync state
  private dbRowId: string | null = null;
  private insertingPlayer = false;
  private lastSynced: PlayerInit | null = null;
  latestState: PlayerInit | null = null;

  // Reconciliation
  private hasReconciled = false;

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

  constructor(
    private db: ReturnType<typeof useDb>,
    private playerId: string,
  ) {}

  // --- Public API (called from game callbacks) ---

  collectDeposit(id: string): void {
    const dep = this.inputs.allDepositsRaw.find((d) => d.id === id);
    if (!dep) return;

    // Record fuelType+positionX now while the deposit is still in allDepositsRaw.
    // Provides a fallback for releaseDeposit/shareFuel before React re-renders.
    this.collectedByThis.set(id, { fuelType: dep.fuelType, positionX: dep.positionX });

    // Sync update: preserves the row ID so the server can forward WHERE EXIT
    // to remote clients' where({collected:false}) subscriptions.
    // Uses db.update (not updateDurable) so the write is emitted to the bridge
    // outbox immediately — updateDurable on the main thread holds writes until
    // an edge-tier ack that never arrives (main thread has no durability tier).
    this.db.update(app.fuel_deposits, id, { collected: true, collectedBy: this.playerId });
  }

  refuel(fuelType: FuelType): void {
    this.releaseDeposit(fuelType).catch(console.error);
  }

  shareFuel(fuelType: string, receiverPlayerId: string): void {
    const shareId =
      this.inputs.allDepositsRaw.find(
        (d) =>
          d.collected &&
          d.collectedBy === this.playerId &&
          d.fuelType === fuelType &&
          !this.releasingIds.has(d.id),
      )?.id ??
      [...this.collectedByThis.entries()].find(
        ([id, info]) => info.fuelType === fuelType && !this.releasingIds.has(id),
      )?.[0];

    if (!shareId) return;
    this.collectedByThis.delete(shareId);
    // Sync update: same reason as collectDeposit — updateDurable hangs on main thread.
    this.db.update(app.fuel_deposits, shareId, { collectedBy: receiverPlayerId });
  }

  burstDeposit(fuelType: string): void {
    this.releaseDeposit(fuelType).catch(console.error);
  }

  sendMessage(text: string): void {
    this.db
      .insertDurable(
        app.chat_messages,
        { playerId: this.playerId, message: text, createdAt: Math.floor(Date.now() / 1000) },
        { tier: "edge" },
      )
      .catch(console.error);
  }

  updateState(state: PlayerInit): void {
    this.latestState = state;
    if (!this.dbRowId) return;
    if (this.lastSynced && !playerStateChanged(this.lastSynced, state)) return;
    this.lastSynced = { ...state };
    this.db.updateDurable(app.players, this.dbRowId, state, { tier: "edge" }).catch(console.error);
  }

  setInputs(inputs: SyncInputs): void {
    this.inputs = inputs;

    // Resolve dbRowId from subscription; write current state immediately
    if (!this.dbRowId && inputs.localPlayerRows.length > 0) {
      this.dbRowId = inputs.localPlayerRows[0].id;
      if (this.latestState) {
        this.lastSynced = { ...this.latestState };
        this.db
          .updateDurable(app.players, this.dbRowId, this.latestState, { tier: "edge" })
          .catch(console.error);
      }
    }

    if (inputs.settled) {
      // Reconcile deposits exactly once
      if (!this.hasReconciled) {
        this.hasReconciled = true;
        reconcileDeposits(this.db, inputs.uncollectedDeposits, inputs.perTypeLimits).catch(
          console.error,
        );
      }

      // Insert player row if not yet created
      if (!this.dbRowId && !this.insertingPlayer && this.latestState) {
        this.insertingPlayer = true;
        const state = this.latestState;
        this.db
          .insertDurable(app.players, state, { tier: "edge" })
          .then((row) => {
            if (!this.dbRowId) {
              this.dbRowId = row.id;
              this.lastSynced = { ...state };
            }
          })
          .catch(console.error)
          .finally(() => {
            this.insertingPlayer = false;
          });
      }
    }

    // Release stale collected deposits when the game is restarting
    const mode = this.latestState?.mode;
    if (mode === "start" || mode === "descending") {
      for (const d of inputs.allDepositsRaw) {
        if (d.collected && d.collectedBy === this.playerId && !this.releasingIds.has(d.id)) {
          this.releasingIds.add(d.id);
          this.collectedByThis.delete(d.id);
          this.db
            .updateDurable(
              app.fuel_deposits,
              d.id,
              { collected: false, collectedBy: "" },
              { tier: "edge" },
            )
            .finally(() => this.releasingIds.delete(d.id))
            .catch(console.error);
        }
      }
      for (const [id] of Array.from(this.collectedByThis.entries())) {
        if (!this.releasingIds.has(id)) {
          this.releasingIds.add(id);
          this.collectedByThis.delete(id);
          this.db
            .updateDurable(
              app.fuel_deposits,
              id,
              { collected: false, collectedBy: "" },
              { tier: "edge" },
            )
            .finally(() => this.releasingIds.delete(id))
            .catch(console.error);
        }
      }
    }
  }

  destroy(): void {}

  /**
   * Reset a collected deposit of the given fuel type (owned by this player) so
   * it can be collected again.
   *
   * Uses DELETE+INSERT rather than UPDATE collected:true→false because
   * edge-tier INSERT fires WHERE ENTRY reliably for where({collected:false})
   * subscriptions, whereas UPDATE-based re-entry does not.
   */
  private async releaseDeposit(fuelType: string): Promise<void> {
    const dep = this.inputs.allDepositsRaw.find(
      (d) =>
        d.collected &&
        d.collectedBy === this.playerId &&
        d.fuelType === fuelType &&
        !this.releasingIds.has(d.id),
    );

    let depId: string | undefined;
    let positionX: number | undefined;

    if (dep) {
      depId = dep.id;
      positionX = dep.positionX;
    } else {
      for (const [id, info] of this.collectedByThis) {
        if (info.fuelType === fuelType && !this.releasingIds.has(id)) {
          depId = id;
          positionX = info.positionX;
          break;
        }
      }
    }

    if (!depId || positionX === undefined) return;

    this.releasingIds.add(depId);
    this.collectedByThis.delete(depId);
    try {
      // Sync delete + insert updates the local store immediately (immediate_tick),
      // firing WHERE ENTRY on the where({collected:false}) subscription without
      // waiting for a durability ack that never arrives on the main thread.
      // forward_update_to_servers still propagates both ops cross-client.
      this.db.delete(app.fuel_deposits, depId);
      this.db.insert(app.fuel_deposits, {
        fuelType,
        positionX,
        createdAt: Math.floor(Date.now() / 1000),
        collected: false,
        collectedBy: "",
      });
    } finally {
      this.releasingIds.delete(depId);
    }
  }
}

// ---------------------------------------------------------------------------
// Pure helpers (exported for tests)
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

  const byType = new Map<string, FuelDeposit[]>();
  for (const ft of FUEL_TYPES) byType.set(ft, []);
  for (const d of uncollectedDeposits) {
    const arr = byType.get(d.fuelType);
    if (arr) arr.push(d);
  }

  // Fire all inserts and trims concurrently so the ServerPayloadBatcher can
  // accumulate them into a single POST rather than N sequential round-trips.
  const ops: Promise<unknown>[] = [];

  for (let i = 0; i < FUEL_TYPES.length; i++) {
    const ft = FUEL_TYPES[i];
    const deposits = byType.get(ft) ?? [];
    const target = perTypeLimits[i];
    const diff = target - deposits.length;

    if (diff > 0) {
      for (let j = 0; j < diff; j++) {
        // Deterministic seed so concurrent clients generate the same positions
        const slotSeed = i * 1000 + deposits.length + j;
        ops.push(
          db.insertDurable(
            app.fuel_deposits,
            {
              fuelType: ft,
              positionX: Math.floor(seededRand(slotSeed) * MOON_SURFACE_WIDTH),
              createdAt: nowS,
              collected: false,
              collectedBy: "",
            },
            { tier: "edge" },
          ),
        );
      }
    } else if (diff < 0) {
      // Trim excess: remove the newest deposits first (highest createdAt)
      const sorted = [...deposits].sort((a, b) => b.createdAt - a.createdAt);
      for (let j = 0; j < -diff; j++) {
        ops.push(
          db.updateDurable(
            app.fuel_deposits,
            sorted[j].id,
            { collected: true, collectedBy: "__trimmed__" },
            { tier: "edge" },
          ),
        );
      }
    }
  }

  await Promise.all(ops);
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

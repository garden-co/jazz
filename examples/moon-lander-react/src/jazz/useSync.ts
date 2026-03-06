/**
 * useSync — all Jazz reads for the game, in one place.
 *
 * Jazz pattern used here:
 *   - useDb()     — access the Jazz DB client for writes (passed to SyncManager)
 *   - useAll(query) — live subscription to a query; re-renders on any change;
 *                    results stream from the server, enabling real-time cross-client updates.
 *
 * Three tables are subscribed:
 *   app.players          — all other players' positions, modes, fuel levels
 *   app.fuel_deposits    — which deposits are on the surface and who collected them
 *   app.chat_messages    — recent chat messages
 *
 * Writes go through SyncManager (see SyncManager.ts), which batches them on a
 * 200ms interval to avoid write storms on every physics frame.
 */

import { useAll, useDb } from "jazz-tools/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { FuelDeposit, Player, PlayerInit, ChatMessage } from "../../schema/app";
import { app } from "../../schema/app";
import type { FuelType } from "../game/constants";
import { FUEL_TYPES, STALE_THRESHOLD_S } from "../game/constants";
import type { Deposit } from "../game/types";
import { DEPOSITS_PER_TYPE, SyncManager } from "./SyncManager";
import type { SyncInputs } from "./SyncManager";

/** Epoch-seconds cutoff below which a player is stale, refreshed every 30s. */
function useStaleCutoff(): number {
  const [cutoff, setCutoff] = useState(() => Math.floor(Date.now() / 1000) - STALE_THRESHOLD_S);
  useEffect(() => {
    const id = setInterval(
      () => setCutoff(Math.floor(Date.now() / 1000) - STALE_THRESHOLD_S),
      30_000,
    );
    return () => clearInterval(id);
  }, []);
  return cutoff;
}

export interface SyncResult {
  // Game props derived from Jazz subscriptions
  deposits: Deposit[];
  inventory: FuelType[];
  remotePlayers: Player[];
  chatMessages: ChatMessage[];

  // Write callbacks (forwarded to SyncManager)
  collectDeposit: (id: string) => void;
  refuel: (fuelType: FuelType) => void;
  shareFuel: (fuelType: string, receiverPlayerId: string) => void;
  burstDeposit: (fuelType: string) => void;
  sendMessage: (text: string) => void;
  updateState: (state: PlayerInit) => void;

  // DebugPanel props
  syncInputs: SyncInputs;
  remotePlayerCount: number;
  chatMessageCount: number;
  gameState: PlayerInit | null;
}

export function useSync(playerId: string): SyncResult {
  const db = useDb();
  const staleCutoff = useStaleCutoff();

  // ---------------------------------------------------------------------------
  // Subscriptions — useAll streams live results from the server.
  // undefined = not yet settled (subscription hasn't received its first result).
  // ---------------------------------------------------------------------------

  // Other players (exclude self)
  const allRemotePlayers = useAll(app.players.where({ playerId: { ne: playerId } })) ?? [];
  const remotePlayers = useMemo(
    () => allRemotePlayers.filter((p) => p.lastSeen > staleCutoff),
    [allRemotePlayers, staleCutoff],
  );

  // Chat messages (ordered oldest-first for rendering)
  const allChatMessages = useAll(app.chat_messages.orderBy("createdAt", "asc")) ?? [];

  // Local player row — used to detect first join (no row yet) vs reconnect
  const localPlayerRowsRaw = useAll(app.players.where({ playerId }));
  const localPlayerRows = localPlayerRowsRaw ?? [];
  const localFuelType = localPlayerRows[0]?.requiredFuelType ?? FUEL_TYPES[0];

  // Per-type deposit limits: base + 1 extra for each player whose required type
  // matches (ensures every player has a deposit of their required fuel available)
  const perTypeLimits = useMemo(() => {
    const counts = new Map<string, number>();
    for (const ft of FUEL_TYPES) counts.set(ft, DEPOSITS_PER_TYPE);
    counts.set(localFuelType, (counts.get(localFuelType) ?? DEPOSITS_PER_TYPE) + 1);
    for (const p of remotePlayers) {
      if (p.requiredFuelType) {
        counts.set(p.requiredFuelType, (counts.get(p.requiredFuelType) ?? DEPOSITS_PER_TYPE) + 1);
      }
    }
    return FUEL_TYPES.map((ft) => counts.get(ft) ?? DEPOSITS_PER_TYPE);
  }, [remotePlayers, localFuelType]);

  // Uncollected deposits — what the game renders on the surface.
  // "edge" tier: undefined until the edge subscription connects, which drives settled detection.
  const allUncollected = useAll(app.fuel_deposits.where({ collected: false }), "edge");

  // This player's collected deposits (compound WHERE = precise local tracking).
  // WHERE ENTRY fires immediately when this player collects (both fields match).
  const localCollectedDeposits = useAll(
    app.fuel_deposits.where({ collected: true, collectedBy: playerId }),
  );

  // All collected deposits — broader subscription used to receive shares from
  // other players. When Player A shares with B, B already has the row here
  // (it entered when A collected it), so collectedBy updating to B propagates
  // as a plain row update without needing WHERE re-evaluation.
  const allCollectedDeposits = useAll(app.fuel_deposits.where({ collected: true }));

  const settled = allUncollected !== undefined;
  const uncollectedDeposits = allUncollected ?? [];
  const myCollectedDeposits = localCollectedDeposits ?? [];

  // Merge local + broad collected subscriptions so received shares show in inventory
  const effectiveMyCollected = useMemo(() => {
    const map = new Map<string, FuelDeposit>();
    for (const d of myCollectedDeposits) map.set(d.id, d);
    for (const d of (allCollectedDeposits ?? []).filter((d) => d.collectedBy === playerId)) {
      map.set(d.id, d);
    }
    return [...map.values()] as FuelDeposit[];
  }, [myCollectedDeposits, allCollectedDeposits, playerId]);

  const allDepositsRaw = useMemo(
    () => [...uncollectedDeposits, ...myCollectedDeposits] as FuelDeposit[],
    [uncollectedDeposits, myCollectedDeposits],
  );

  const perTypeCounts = useMemo(() => {
    const counts = Array.from({ length: FUEL_TYPES.length }, () => 0);
    for (const d of uncollectedDeposits) {
      const idx = FUEL_TYPES.indexOf(d.fuelType as FuelType);
      if (idx >= 0) counts[idx]++;
    }
    return counts;
  }, [uncollectedDeposits]);

  // ---------------------------------------------------------------------------
  // Derived game props
  // ---------------------------------------------------------------------------

  // Track when each deposit ID was first seen for fade-in animation
  const depositSpawnTimesRef = useRef<Map<string, number>>(new Map());

  const deposits = useMemo(() => {
    const spawnTimes = depositSpawnTimesRef.current;
    const now = performance.now() / 1000;
    const activeIds = new Set<string>();
    for (const d of uncollectedDeposits) {
      activeIds.add(d.id);
      if (!spawnTimes.has(d.id)) spawnTimes.set(d.id, now);
    }
    for (const id of spawnTimes.keys()) {
      if (!activeIds.has(id)) spawnTimes.delete(id);
    }
    return uncollectedDeposits.map((d) => ({
      id: d.id,
      x: d.positionX,
      type: d.fuelType as FuelType,
      spawnTime: spawnTimes.get(d.id) ?? now,
    }));
  }, [uncollectedDeposits]);

  const inventory = useMemo(
    () => effectiveMyCollected.map((d) => d.fuelType as FuelType),
    [effectiveMyCollected],
  );

  const chatMessages = useMemo(() => {
    if (!allChatMessages) return [];
    const nowS = Math.floor(Date.now() / 1000);
    return allChatMessages.filter((m) => nowS - m.createdAt < 60);
  }, [allChatMessages]);

  // ---------------------------------------------------------------------------
  // SyncManager — owns all DB writes, batched on a 200ms interval
  // ---------------------------------------------------------------------------

  const syncRef = useRef<SyncManager | null>(null);
  if (!syncRef.current) syncRef.current = new SyncManager(db, playerId);
  const sync = syncRef.current;
  useEffect(() => () => sync.destroy(), [sync]);

  sync.setInputs({
    settled,
    localPlayerSettled: localPlayerRowsRaw !== undefined,
    uncollectedDeposits,
    myCollectedDeposits: effectiveMyCollected,
    allDepositsRaw,
    localPlayerRows,
    perTypeLimits,
    perTypeCounts,
    myCollectedCount: effectiveMyCollected.length,
    debugTotalDeposits: allDepositsRaw.length,
  });

  return {
    deposits,
    inventory,
    remotePlayers,
    chatMessages,

    collectDeposit: (id) => sync.collectDeposit(id),
    refuel: (ft) => sync.refuel(ft),
    shareFuel: (ft, rpId) => sync.shareFuel(ft, rpId),
    burstDeposit: (ft) => sync.burstDeposit(ft),
    sendMessage: (text) => sync.sendMessage(text),
    updateState: (state) => sync.updateState(state),

    syncInputs: sync.inputs,
    remotePlayerCount: remotePlayers.length,
    chatMessageCount: allChatMessages?.length ?? 0,
    gameState: sync.latestState,
  };
}

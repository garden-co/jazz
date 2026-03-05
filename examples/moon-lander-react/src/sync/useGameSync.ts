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

// ---------------------------------------------------------------------------
// useGameSync — all Jazz reads, derivations, and writes for the game
// ---------------------------------------------------------------------------

export interface GameSyncResult {
  // Game props
  deposits: Deposit[];
  inventory: FuelType[];
  remotePlayers: Player[];
  chatMessages: ChatMessage[];

  // SyncManager callbacks
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

export function useGameSync(playerId: string): GameSyncResult {
  const db = useDb();
  const staleCutoff = useStaleCutoff();

  // --- Subscriptions ---
  const allRemotePlayers = useAll(app.players.where({ playerId: { ne: playerId } }), "edge") ?? [];

  // Filter stale players in JS — the cutoff changes every 30s, so doing this
  // client-side avoids constant query re-subscriptions.
  const remotePlayers = useMemo(
    () => allRemotePlayers.filter((p) => p.lastSeen > staleCutoff),
    [allRemotePlayers, staleCutoff],
  );
  const allChatMessages = useAll(app.chat_messages.orderBy("createdAt", "asc"), "edge") ?? [];

  const localPlayerRowsRaw = useAll(app.players.where({ playerId }));
  const localPlayerRows = localPlayerRowsRaw ?? [];
  const localFuelType = localPlayerRows[0]?.requiredFuelType ?? FUEL_TYPES[0];

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

  const allUncollected = useAll(app.fuel_deposits.where({ collected: false }), "edge");

  // Compound WHERE: only this player's collected deposits.
  // Fires WHERE ENTRY immediately for local writes (both fields match the write
  // {collected:true, collectedBy:playerId}) without interference from the broader
  // all-collected subscription.  Used as the source of truth for burst/release.
  const localCollectedDeposits = useAll(
    app.fuel_deposits.where({ collected: true, collectedBy: playerId }),
    "edge",
  );

  // Broad subscription: all collected deposits.
  // WHERE ENTRY fires for cross-client boolean writes (A collects → false→true,
  // B sees it here). When A then shares (collectedBy: A→B), B already has the
  // row in this subscription and receives it as a plain row update.
  const allCollectedDeposits = useAll(app.fuel_deposits.where({ collected: true }), "edge");
  // settled: edge subscription has returned its initial result.
  const settled = allUncollected !== undefined;

  const uncollectedDeposits = allUncollected ?? [];

  // myCollectedDeposits: reliable local tracking via compound WHERE.
  // Used for allDepositsRaw so burst/release can always find this player's
  // collected deposits quickly.
  const myCollectedDeposits = localCollectedDeposits ?? [];

  // effectiveMyCollected: merges local + broad subscriptions so that received
  // shares (which enter via allCollectedDeposits) appear in the inventory.
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

  // Track when each deposit ID was first seen (monotonic seconds) for fade-in
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

  const inventory = useMemo(() => {
    return effectiveMyCollected.map((d) => d.fuelType as FuelType);
  }, [effectiveMyCollected]);

  // --- SyncManager ---
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

  // --- View derivations ---
  const chatMessages = useMemo(() => {
    if (!allChatMessages) return [];
    const nowS = Math.floor(Date.now() / 1000);
    return allChatMessages.filter((m) => nowS - m.createdAt < 60);
  }, [allChatMessages]);

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

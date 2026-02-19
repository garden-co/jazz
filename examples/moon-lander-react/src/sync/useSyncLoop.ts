import type { useDb } from "jazz-tools/react";
import { useCallback, useEffect, useRef } from "react";
import type { PlayerInit } from "../../schema/app";
import { app } from "../../schema/app";
import type { FuelType } from "../game/constants";
import { DB_SYNC_INTERVAL_MS } from "../game/constants";
import type { DepositState } from "./useDeposits";
import { playerStateChanged, reconcileDeposits } from "./writes";

// ---------------------------------------------------------------------------
// useSyncLoop
// ---------------------------------------------------------------------------

export function useSyncLoop(
  db: ReturnType<typeof useDb>,
  playerId: string,
  depositState: DepositState,
): {
  gameCallbacks: {
    onCollectDeposit: (id: string) => void;
    onRefuel: (fuelType: FuelType) => void;
    onShareFuel: (fuelType: string, receiverPlayerId: string) => void;
    onBurstDeposit: (fuelType: string) => void;
    onSendMessage: (text: string) => void;
    onStateChange: (state: PlayerInit) => void;
  };
  latestStateRef: React.RefObject<PlayerInit | null>;
} {
  const depositStateRef = useRef(depositState);
  depositStateRef.current = depositState;

  const latestStateRef = useRef<PlayerInit | null>(null);
  const handleStateChange = useCallback((state: PlayerInit) => {
    latestStateRef.current = state;
  }, []);

  const pendingCollectionsRef = useRef<string[]>([]);
  const handleCollectDeposit = useCallback((id: string) => {
    pendingCollectionsRef.current.push(id);
  }, []);

  const pendingRefuelsRef = useRef<FuelType[]>([]);
  const handleRefuel = useCallback((fuelType: FuelType) => {
    pendingRefuelsRef.current.push(fuelType);
  }, []);

  const pendingSharesRef = useRef<
    Array<{ fuelType: string; receiverPlayerId: string }>
  >([]);
  const handleShareFuel = useCallback(
    (fuelType: string, receiverPlayerId: string) => {
      pendingSharesRef.current.push({ fuelType, receiverPlayerId });
    },
    [],
  );

  const pendingBurstsRef = useRef<string[]>([]);
  const handleBurstDeposit = useCallback((fuelType: string) => {
    pendingBurstsRef.current.push(fuelType);
  }, []);

  const pendingMessagesRef = useRef<string[]>([]);
  const handleSendMessage = useCallback((text: string) => {
    pendingMessagesRef.current.push(text);
  }, []);

  useEffect(() => {
    let dbRowId: string | null = null;
    let lastSynced: PlayerInit | null = null;
    let hasReconciled = false;

    const id = setInterval(async () => {
      const ds = depositStateRef.current;

      // Reconcile deposits once after edge subscriptions settle
      if (ds.settled && !hasReconciled) {
        hasReconciled = true;
        await reconcileDeposits(db, ds.uncollectedRaw, ds.perTypeLimits);
      }

      // Sync player state (insert or update)
      const state = latestStateRef.current;
      if (state) {
        if (!dbRowId && ds.localPlayerRows.length > 0) {
          dbRowId = ds.localPlayerRows[0].id;
        }
        if (dbRowId) {
          if (!lastSynced || playerStateChanged(lastSynced, state)) {
            await db.updatePersisted(app.players, dbRowId, state, "edge");
            lastSynced = { ...state };
          }
        } else if (ds.settled) {
          dbRowId = await db.insertPersisted(app.players, state, "edge");
          lastSynced = { ...state };
        }
      }

      // Flush deposit collections
      for (const depId of pendingCollectionsRef.current.splice(0)) {
        await db.updatePersisted(
          app.fuel_deposits,
          depId,
          { collected: true, collectedBy: playerId },
          "edge",
        );
      }

      // Flush deposit releases (refuel + burst)
      for (const fuelType of pendingRefuelsRef.current.splice(0)) {
        const dep = ds.allDepositsRaw.find(
          (d) =>
            d.collected &&
            d.collectedBy === playerId &&
            d.fuelType === fuelType,
        );
        if (dep) {
          await db.updatePersisted(
            app.fuel_deposits,
            dep.id,
            { collected: false, collectedBy: "" },
            "edge",
          );
        }
      }
      for (const fuelType of pendingBurstsRef.current.splice(0)) {
        const dep = ds.allDepositsRaw.find(
          (d) =>
            d.collected &&
            d.collectedBy === playerId &&
            d.fuelType === fuelType,
        );
        if (dep) {
          await db.updatePersisted(
            app.fuel_deposits,
            dep.id,
            { collected: false, collectedBy: "" },
            "edge",
          );
        }
      }

      // Flush fuel shares
      for (const share of pendingSharesRef.current.splice(0)) {
        const dep = ds.allDepositsRaw.find(
          (d) =>
            d.collected &&
            d.collectedBy === playerId &&
            d.fuelType === share.fuelType,
        );
        if (dep) {
          await db.updatePersisted(
            app.fuel_deposits,
            dep.id,
            { collectedBy: share.receiverPlayerId },
            "edge",
          );
        }
      }

      // Flush chat messages
      for (const text of pendingMessagesRef.current.splice(0)) {
        await db.insertPersisted(
          app.chat_messages,
          { playerId, message: text, createdAt: Math.floor(Date.now() / 1000) },
          "edge",
        );
      }

      // Release stale collected deposits when the game is restarting
      const mode = state?.mode;
      if (mode === "start" || mode === "descending") {
        for (const d of ds.allDepositsRaw) {
          if (d.collected && d.collectedBy === playerId) {
            await db.updatePersisted(
              app.fuel_deposits,
              d.id,
              { collected: false, collectedBy: "" },
              "edge",
            );
          }
        }
      }
    }, DB_SYNC_INTERVAL_MS);

    return () => clearInterval(id);
  }, [db, playerId]);

  return {
    gameCallbacks: {
      onCollectDeposit: handleCollectDeposit,
      onRefuel: handleRefuel,
      onShareFuel: handleShareFuel,
      onBurstDeposit: handleBurstDeposit,
      onSendMessage: handleSendMessage,
      onStateChange: handleStateChange,
    },
    latestStateRef,
  };
}

import { useRef, useCallback, useMemo } from "react";
import { JazzProvider, useDb, useAll } from "jazz-react";
import type { DbConfig } from "jazz-ts";
import { app } from "../schema/app.js";
import { Game, type RemotePlayer, type GameState, type ChatMessage } from "./Game.js";
import { DB_SYNC_INTERVAL_MS, FUEL_TYPES, MOON_SURFACE_WIDTH } from "./game/constants.js";
import type { FuelType } from "./game/constants.js";
import { useEffect } from "react";

// ---------------------------------------------------------------------------
// GameWithSync — bridges Game ↔ Jazz DB
// ---------------------------------------------------------------------------

function GameWithSync({
  physicsSpeed,
  playerId,
}: {
  physicsSpeed?: number;
  playerId: string;
}) {
  const db = useDb();
  // Jazz-native filtering: only subscribe to remote players (ne = local)
  const remotePlayerRows = useAll(app.players.where({ playerId: { ne: playerId } }));
  // Separate subscription for the local player's row (for finding existing row on reload)
  const localPlayerRows = useAll(app.players.where({ playerId }));
  const allDepositsRaw = useAll(app.fuel_deposits);
  const allChatMessages = useAll(app.chat_messages);

  // Track the Jazz row ID for the local player so we can update (not re-insert)
  const dbRowIdRef = useRef<string | null>(null);
  const localPlayerRowsRef = useRef(localPlayerRows);
  localPlayerRowsRef.current = localPlayerRows;

  // Keep latest deposit subscription accessible from setInterval
  const allDepositsRef = useRef(allDepositsRaw);
  allDepositsRef.current = allDepositsRaw;

  // Buffer latest game state in a ref — written to DB on a separate interval
  // to avoid re-entrant WASM borrows when sync messages trigger React renders
  const latestStateRef = useRef<GameState | null>(null);

  const handleStateChange = useCallback((state: GameState) => {
    latestStateRef.current = state;
  }, []);

  // Pending deposit collections (WASM safety — written in setInterval, not during render)
  const pendingCollectionsRef = useRef<string[]>([]);
  const handleCollectDeposit = useCallback((id: string) => {
    pendingCollectionsRef.current.push(id);
  }, []);

  // Pending refuel consumptions (deposit consumed for lander fuel)
  const pendingRefuelsRef = useRef<FuelType[]>([]);
  const handleRefuel = useCallback((fuelType: FuelType) => {
    pendingRefuelsRef.current.push(fuelType);
  }, []);

  // Pending fuel shares (rewrite collectedBy from local → receiver)
  const pendingSharesRef = useRef<Array<{ fuelType: string; receiverPlayerId: string }>>([]);
  const handleShareFuel = useCallback((fuelType: string, receiverPlayerId: string) => {
    pendingSharesRef.current.push({ fuelType, receiverPlayerId });
  }, []);

  // Pending burst deposits (eject back to surface at new position)
  const pendingBurstsRef = useRef<Array<{ fuelType: string; newX: number }>>([]);
  const handleBurstDeposit = useCallback((fuelType: string, newX: number) => {
    pendingBurstsRef.current.push({ fuelType, newX });
  }, []);

  // Pending chat messages
  const pendingMessagesRef = useRef<string[]>([]);
  const handleSendMessage = useCallback((text: string) => {
    pendingMessagesRef.current.push(text);
  }, []);

  // Seed flag — prevents re-seeding after initial population.
  // Grace period lets the subscription deliver existing data from OPFS/server
  // before we decide the table is empty.
  const seededRef = useRef(false);
  const mountedAtRef = useRef(Date.now());

  // Flush all DB writes in a single setInterval (player sync + deposit collection + seeding)
  useEffect(() => {
    const id = setInterval(() => {
      const GRACE_MS = 2000;
      const elapsed = Date.now() - mountedAtRef.current;

      // --- Seed deposits if DB is empty (after grace period) ---
      if (!seededRef.current && allDepositsRef.current) {
        if (allDepositsRef.current.length > 0) {
          seededRef.current = true;
        } else if (elapsed > GRACE_MS) {
          seededRef.current = true;
          const nowS = Math.floor(Date.now() / 1000);
          for (const fuelType of FUEL_TYPES) {
            for (let i = 0; i < 3; i++) {
              db.insert(app.fuel_deposits, {
                fuelType,
                positionX: Math.floor(Math.random() * MOON_SURFACE_WIDTH),
                createdAt: nowS,
                collected: false,
                collectedBy: "",
              });
            }
          }
        }
      }

      // --- Player state sync (after grace period to find existing row) ---
      const state = latestStateRef.current;
      if (state) {
        const playerData = {
          playerId,
          name: state.playerName,
          color: state.playerColor,
          mode: state.mode,
          online: true,
          lastSeen: Math.floor(Date.now() / 1000),
          positionX: state.positionX,
          positionY: state.positionY,
          velocityX: state.velocityX,
          velocityY: state.velocityY,
          requiredFuelType: state.requiredFuelType,
          landerFuelLevel: state.fuel,
          landerSpawnX: state.landerSpawnX,
        };

        if (!dbRowIdRef.current && localPlayerRowsRef.current.length > 0) {
          dbRowIdRef.current = localPlayerRowsRef.current[0].id;
        }

        // Wait for grace period before inserting a new row — gives the
        // subscription time to deliver the existing row from OPFS/server.
        if (dbRowIdRef.current) {
          db.update(app.players, dbRowIdRef.current, playerData);
        } else if (elapsed > GRACE_MS) {
          dbRowIdRef.current = db.insert(app.players, playerData);
        }
      }

      // --- Deposit collection writes ---
      for (const depId of pendingCollectionsRef.current.splice(0)) {
        db.update(app.fuel_deposits, depId, {
          collected: true,
          collectedBy: playerId,
        });
      }

      // --- Refuel consumption writes ---
      // Mark consumed deposits as used: collectedBy="" keeps collected=true
      // so they don't reappear on the surface or in any player's inventory
      for (const fuelType of pendingRefuelsRef.current.splice(0)) {
        const deposits = allDepositsRef.current;
        if (!deposits) continue;
        const dep = deposits.find(
          (d) => d.collected && d.collectedBy === playerId && d.fuelType === fuelType,
        );
        if (dep) {
          db.update(app.fuel_deposits, dep.id, {
            collectedBy: "",
          });
        }
      }

      // --- Share writes ---
      // Transfer fuel: rewrite collectedBy from local player to receiver
      for (const share of pendingSharesRef.current.splice(0)) {
        const deposits = allDepositsRef.current;
        if (!deposits) continue;
        const dep = deposits.find(
          (d) => d.collected && d.collectedBy === playerId && d.fuelType === share.fuelType,
        );
        if (dep) {
          db.update(app.fuel_deposits, dep.id, {
            collectedBy: share.receiverPlayerId,
          });
        }
      }

      // --- Burst writes ---
      // Eject deposits back to surface: mark uncollected at new position
      for (const burst of pendingBurstsRef.current.splice(0)) {
        const deposits = allDepositsRef.current;
        if (!deposits) continue;
        const dep = deposits.find(
          (d) => d.collected && d.collectedBy === playerId && d.fuelType === burst.fuelType,
        );
        if (dep) {
          db.update(app.fuel_deposits, dep.id, {
            collected: false,
            collectedBy: "",
            positionX: Math.floor(burst.newX),
          });
        }
      }
      // --- Chat message writes ---
      for (const text of pendingMessagesRef.current.splice(0)) {
        db.insert(app.chat_messages, {
          playerId,
          message: text,
          createdAt: Math.floor(Date.now() / 1000),
        });
      }
    }, DB_SYNC_INTERVAL_MS);

    return () => clearInterval(id);
  }, [db, playerId]);

  // Map Jazz deposit subscription → Deposit[] for Game (uncollected only)
  const deposits = useMemo(() => {
    if (!allDepositsRaw) return [];
    return allDepositsRaw
      .filter((d) => !d.collected)
      .map((d) => ({
        id: d.id,
        x: d.positionX,
        type: d.fuelType as FuelType,
      }));
  }, [allDepositsRaw]);

  // Derive inventory from Jazz: fuel types where collectedBy = this player
  const inventory = useMemo(() => {
    if (!allDepositsRaw) return undefined;
    return allDepositsRaw
      .filter((d) => d.collected && d.collectedBy === playerId)
      .map((d) => d.fuelType as FuelType);
  }, [allDepositsRaw, playerId]);

  // Map Jazz chat messages → ChatMessage[] for Game (recent only)
  const chatMessages: ChatMessage[] = useMemo(() => {
    if (!allChatMessages) return [];
    const nowS = Math.floor(Date.now() / 1000);
    return allChatMessages
      .filter((m) => nowS - m.createdAt < 60) // only last 60 seconds
      .map((m) => ({
        id: m.id,
        playerId: m.playerId,
        message: m.message,
        createdAt: m.createdAt,
      }));
  }, [allChatMessages]);

  // Map Jazz subscription → RemotePlayer[] for Game.
  // Jazz query already excludes the local player (ne filter).
  const remotePlayers: RemotePlayer[] = useMemo(() => {
    return remotePlayerRows.map((p) => ({
      id: p.id,
      name: p.name,
      mode: p.mode as RemotePlayer["mode"],
      positionX: p.positionX,
      positionY: p.positionY,
      velocityX: p.velocityX,
      velocityY: p.velocityY,
      color: p.color,
      requiredFuelType: p.requiredFuelType,
      lastSeen: p.lastSeen,
      landerFuelLevel: p.landerFuelLevel,
      playerId: p.playerId,
      landerX: p.landerSpawnX,
    }));
  }, [remotePlayerRows]);

  return (
    <Game
      physicsSpeed={physicsSpeed}
      remotePlayers={remotePlayers}
      deposits={deposits}
      inventory={inventory}
      chatMessages={chatMessages}
      onCollectDeposit={handleCollectDeposit}
      onRefuel={handleRefuel}
      onShareFuel={handleShareFuel}
      onBurstDeposit={handleBurstDeposit}
      onSendMessage={handleSendMessage}
      onStateChange={handleStateChange}
    />
  );
}

// ---------------------------------------------------------------------------
// App — wraps Game in JazzProvider when config is provided
// ---------------------------------------------------------------------------

interface AppProps {
  config?: DbConfig;
  playerId?: string;
  physicsSpeed?: number;
}

export function App({ config, playerId, physicsSpeed }: AppProps) {
  // No config → standalone Game (Phase 1 compatibility)
  if (!config) {
    return <Game physicsSpeed={physicsSpeed} />;
  }

  return (
    <JazzProvider config={config}>
      <GameWithSync physicsSpeed={physicsSpeed} playerId={playerId ?? crypto.randomUUID()} />
    </JazzProvider>
  );
}

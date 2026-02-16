import { useRef, useCallback, useMemo } from "react";
import { JazzProvider, useDb, useAll } from "jazz-react";
import type { DbConfig } from "jazz-ts";
import { app } from "../schema/app.js";
import { Game, type RemotePlayer, type GameState } from "./Game.js";
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
  const allPlayers = useAll(app.players);
  const allDepositsRaw = useAll(app.fuel_deposits);

  // Track the Jazz row ID for the local player so we can update (not re-insert)
  const dbRowIdRef = useRef<string | null>(null);
  const allPlayersRef = useRef(allPlayers);
  allPlayersRef.current = allPlayers;

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

  // Seed flag — prevents re-seeding after initial population
  const seededRef = useRef(false);

  // Flush all DB writes in a single setInterval (player sync + deposit collection + seeding)
  useEffect(() => {
    const id = setInterval(() => {
      // --- Seed deposits if DB is empty ---
      if (!seededRef.current && allDepositsRef.current && allDepositsRef.current.length === 0) {
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

      // --- Player state sync ---
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

        if (!dbRowIdRef.current && allPlayersRef.current) {
          const existing = allPlayersRef.current.find((p) => p.playerId === playerId);
          if (existing) {
            dbRowIdRef.current = existing.id;
          }
        }

        if (!dbRowIdRef.current) {
          dbRowIdRef.current = db.insert(app.players, playerData);
        } else {
          db.update(app.players, dbRowIdRef.current, playerData);
        }
      }

      // --- Deposit collection writes ---
      for (const depId of pendingCollectionsRef.current.splice(0)) {
        db.update(app.fuel_deposits, depId, {
          collected: true,
          collectedBy: playerId,
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

  // Map Jazz subscription data → RemotePlayer[] for Game
  // Filter by playerId so we exclude all our own rows (current + any stale)
  const remotePlayers: RemotePlayer[] = useMemo(() => {
    if (!allPlayers) return [];
    return allPlayers
      .filter((p) => p.playerId !== playerId)
      .map((p) => ({
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
        landerX: p.landerSpawnX || undefined,
      }));
  }, [allPlayers, playerId]);

  return (
    <Game
      physicsSpeed={physicsSpeed}
      remotePlayers={remotePlayers}
      deposits={deposits}
      onCollectDeposit={handleCollectDeposit}
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

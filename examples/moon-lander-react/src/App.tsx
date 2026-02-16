import { useRef, useCallback, useEffect, useMemo } from "react";
import { JazzProvider, useDb, useAll } from "jazz-react";
import type { DbConfig } from "jazz-ts";
import { app } from "../schema/app.js";
import { Game, type RemotePlayer, type GameState } from "./Game.js";
import { DB_SYNC_INTERVAL_MS } from "./game/constants.js";

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

  // Track the Jazz row ID for the local player so we can update (not re-insert)
  const dbRowIdRef = useRef<string | null>(null);
  const allPlayersRef = useRef(allPlayers);
  allPlayersRef.current = allPlayers;

  // Buffer latest game state in a ref — written to DB on a separate interval
  // to avoid re-entrant WASM borrows when sync messages trigger React renders
  const latestStateRef = useRef<GameState | null>(null);

  const handleStateChange = useCallback((state: GameState) => {
    latestStateRef.current = state;
  }, []);

  useEffect(() => {
    const id = setInterval(() => {
      const state = latestStateRef.current;
      if (!state) return;

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
    }, DB_SYNC_INTERVAL_MS);

    return () => clearInterval(id);
  }, [db, playerId]);

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

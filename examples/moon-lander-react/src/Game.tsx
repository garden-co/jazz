import { useMemo, useRef, useEffect } from "react";
import { useGameEngine } from "./game/engine.js";
import type { RemotePlayerView, Deposit } from "./game/engine.js";
import { getOrCreatePlayerId, derivePlayerProps } from "./game/player.js";
import { Hud } from "./game/Hud.js";
import type { PlayerMode, FuelType } from "./game/constants.js";

// ---------------------------------------------------------------------------
// Types — the contract between Game and the Jazz sync layer (App.tsx)
// ---------------------------------------------------------------------------

export type { PlayerMode, FuelType };

/** State pushed to Jazz on each sync tick. */
export interface GameState {
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  fuel: number;
  landerSpawnX: number;
  playerName: string;
  playerColor: string;
  requiredFuelType: FuelType;
}

/** A remote player received from Jazz and rendered in the game world. */
export interface RemotePlayer {
  id: string;
  name: string;
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  color: string;
  requiredFuelType: string;
  lastSeen: number;
  landerFuelLevel: number;
  playerId?: string;
  landerX?: number;
}

// ---------------------------------------------------------------------------
// Game component
// ---------------------------------------------------------------------------

interface GameProps {
  physicsSpeed?: number;
  remotePlayers?: RemotePlayer[];
  deposits?: Deposit[];
  inventory?: FuelType[];
  onCollectDeposit?: (id: string) => void;
  onRefuel?: (fuelType: FuelType) => void;
  onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
  onStateChange?: (state: GameState) => void;
}

const STALE_THRESHOLD_S = 180; // 3 minutes

export function Game({ physicsSpeed, remotePlayers, deposits, inventory, onCollectDeposit, onRefuel, onShareFuel, onStateChange }: GameProps) {
  const playerId = useRef(getOrCreatePlayerId()).current;
  const playerProps = useRef(derivePlayerProps(playerId)).current;
  const canvasRef = useRef<HTMLCanvasElement>(null);

  // Filter stale remote players (Jazz concern, not engine's job)
  const activeRemotes: RemotePlayerView[] = useMemo(() => {
    if (!remotePlayers) return [];
    const nowS = Math.floor(Date.now() / 1000);
    return remotePlayers
      .filter((rp) => nowS - rp.lastSeen < STALE_THRESHOLD_S)
      .map((rp) => ({
        id: rp.id,
        name: rp.name,
        mode: rp.mode,
        positionX: rp.positionX,
        positionY: rp.positionY,
        velocityY: rp.velocityY,
        color: rp.color,
        landerX: rp.landerX,
        requiredFuelType: rp.requiredFuelType,
        playerId: rp.playerId,
      }));
  }, [remotePlayers]);

  const engine = useGameEngine(canvasRef, {
    physicsSpeed,
    requiredFuelType: playerProps.requiredFuelType,
    remotePlayers: activeRemotes,
    deposits,
    inventory,
    onCollectDeposit,
    onRefuel,
    onShareFuel,
  });

  // Bridge engine state → Jazz sync callback (integers for DB schema)
  useEffect(() => {
    if (!onStateChange) return;
    onStateChange({
      mode: engine.mode,
      positionX: Math.floor(engine.positionX),
      positionY: Math.floor(engine.positionY),
      velocityX: Math.round(engine.velocityX),
      velocityY: Math.round(engine.velocityY),
      fuel: Math.round(engine.fuel),
      landerSpawnX: Math.floor(engine.landerX),
      playerName: playerProps.name,
      playerColor: playerProps.color,
      requiredFuelType: playerProps.requiredFuelType,
    });
  }, [engine, onStateChange, playerProps]);

  return (
    <div
      data-testid="game-container"
      data-player-id={playerId}
      data-player-name={playerProps.name}
      data-player-color={playerProps.color}
      data-required-fuel={playerProps.requiredFuelType}
      data-lander-fuel={engine.fuel}
      data-player-online="true"
      data-player-mode={engine.mode}
      data-player-x={engine.positionX}
      data-player-y={engine.positionY}
      data-velocity-y={engine.velocityY}
      data-lander-x={engine.landerX}
      data-lander-y={engine.landerY}
      data-deposit-count={engine.depositCount}
      data-inventory={engine.inventory.join(",")}
      data-remote-player-count={engine.remotePlayerCount}
      style={{ position: "relative", width: "100vw", height: "100vh" }}
    >
      <canvas
        ref={canvasRef}
        data-testid="game-canvas"
        style={{ display: "block" }}
      />
      <Hud
        mode={engine.mode}
        positionX={engine.positionX}
        positionY={engine.positionY}
        velocityX={engine.velocityX}
        velocityY={engine.velocityY}
        fuel={engine.fuel}
        landerX={engine.landerX}
        requiredFuelType={playerProps.requiredFuelType}
        inventory={engine.inventory}
      />
    </div>
  );
}

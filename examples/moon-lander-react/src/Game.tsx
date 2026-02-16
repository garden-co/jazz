import { useRef, useEffect } from "react";
import { useGameEngine } from "./game/engine.js";
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
  landerX?: number;
}

// ---------------------------------------------------------------------------
// Game component
// ---------------------------------------------------------------------------

interface GameProps {
  physicsSpeed?: number;
  remotePlayers?: RemotePlayer[];
  onStateChange?: (state: GameState) => void;
}

export function Game({ physicsSpeed, remotePlayers, onStateChange }: GameProps) {
  const playerId = useRef(getOrCreatePlayerId()).current;
  const playerProps = useRef(derivePlayerProps(playerId)).current;
  const canvasRef = useRef<HTMLCanvasElement>(null);

  const engine = useGameEngine(canvasRef, {
    physicsSpeed,
    requiredFuelType: playerProps.requiredFuelType,
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

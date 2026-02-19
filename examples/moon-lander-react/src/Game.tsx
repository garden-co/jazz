import { memo, useEffect, useRef, useState } from "react";
import type { ChatMessage, Player, PlayerInit } from "../schema/app";
import styles from "./Game.module.css";
import type { FuelType, PlayerMode } from "./game/constants";
import type { Deposit } from "./game/engine";
import { useGameEngine } from "./game/engine";
import { Hud } from "./game/Hud";
import { derivePlayerProps, getOrCreatePlayerId } from "./game/player";

export type { PlayerMode, FuelType };
export type { ChatMessage, Player, PlayerInit } from "../schema/app";

// ---------------------------------------------------------------------------
// Game component
// ---------------------------------------------------------------------------

interface GameProps {
  playerId?: string;
  physicsSpeed?: number;
  /** Initial player mode. Defaults to "start" (shows start screen). Use "descending" to skip. */
  initialMode?: PlayerMode;
  /** Override the random spawn X position (for tests). */
  spawnX?: number;
  remotePlayers?: Player[];
  deposits?: Deposit[];
  inventory?: FuelType[];
  chatMessages?: ChatMessage[];
  onCollectDeposit?: (id: string) => void;
  onRefuel?: (fuelType: FuelType) => void;
  onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
  onBurstDeposit?: (fuelType: string) => void;
  onSendMessage?: (text: string) => void;
  onStateChange?: (state: PlayerInit) => void;
}

export const Game = memo(function Game({
  playerId: externalPlayerId,
  physicsSpeed,
  initialMode,
  spawnX,
  remotePlayers,
  deposits,
  inventory,
  chatMessages,
  onCollectDeposit,
  onRefuel,
  onShareFuel,
  onBurstDeposit,
  onSendMessage,
  onStateChange,
}: GameProps) {
  const fallbackPlayerId = useRef(getOrCreatePlayerId()).current;
  const playerId = externalPlayerId ?? fallbackPlayerId;
  const playerProps = useRef(derivePlayerProps(playerId)).current;
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const chatInputRef = useRef<HTMLInputElement>(null);

  // Chat state
  const [chatOpen, setChatOpen] = useState(false);
  const chatOpenRef = useRef(false);
  chatOpenRef.current = chatOpen;

  const engine = useGameEngine(canvasRef, {
    physicsSpeed,
    initialMode,
    spawnX,
    requiredFuelType: playerProps.requiredFuelType,
    remotePlayers,
    deposits,
    inventory,
    chatMessages,
    localPlayerId: playerId,
    localPlayerName: playerProps.name,
    localPlayerColor: playerProps.color,
    onCollectDeposit,
    onRefuel,
    onShareFuel,
    onBurstDeposit,
    chatOpen,
  });

  // Bridge engine state → Jazz sync callback (integers for DB schema)
  useEffect(() => {
    if (!onStateChange) return;
    onStateChange({
      playerId,
      name: playerProps.name,
      color: playerProps.color,
      mode: engine.mode,
      online: true,
      lastSeen: Math.floor(Date.now() / 1000),
      positionX: Math.floor(engine.positionX),
      positionY: Math.floor(engine.positionY),
      velocityX: Math.round(engine.velocityX),
      velocityY: Math.round(engine.velocityY),
      requiredFuelType: playerProps.requiredFuelType,
      landerFuelLevel: Math.round(engine.fuel),
      landerSpawnX: Math.floor(engine.landerX),
      thrusting: engine.thrusting,
    });
  }, [engine, onStateChange, playerProps, playerId]);

  const onSendMessageRef = useRef(onSendMessage);
  onSendMessageRef.current = onSendMessage;

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.code === "Escape" && chatOpenRef.current) {
        setChatOpen(false);
        return;
      }
      if (e.code === "Enter") {
        if (chatOpenRef.current) {
          // Send the message and close
          const text = chatInputRef.current?.value.trim() ?? "";
          if (text) {
            onSendMessageRef.current?.(text);
          }
          setChatOpen(false);
        } else {
          // Open chat
          e.preventDefault();
          setChatOpen(true);
          requestAnimationFrame(() => chatInputRef.current?.focus());
        }
        return;
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

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
      data-deposit-count={engine.depositCount}
      data-inventory={engine.inventory.join(",")}
      data-remote-player-count={engine.remotePlayerCount}
      data-share-hint={engine.shareHint ? "true" : "false"}
      data-chat-open={chatOpen ? "true" : "false"}
      className={styles.container}
    >
      <canvas
        ref={canvasRef}
        data-testid="game-canvas"
        className={styles.canvas}
      />
      <Hud
        mode={engine.mode}
        fuel={engine.fuel}
        requiredFuelType={playerProps.requiredFuelType}
        inventory={engine.inventory}
        remotePlayers={remotePlayers ?? []}
        localPlayerName={playerProps.name}
        localPlayerColor={playerProps.color}
      />
      {chatOpen && (
        <input
          ref={chatInputRef}
          data-testid="chat-input"
          type="text"
          maxLength={140}
          autoFocus
          className={styles.chatInput}
          placeholder="Type a message..."
        />
      )}
    </div>
  );
});

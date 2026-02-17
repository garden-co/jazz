import { useMemo, useRef, useEffect, useState } from "react";
import { useGameEngine } from "./game/engine.js";
import type { RemotePlayerView, Deposit } from "./game/engine.js";
import { getOrCreatePlayerId, derivePlayerProps } from "./game/player.js";
import { Hud } from "./game/Hud.js";
import type { PlayerMode, FuelType } from "./game/constants.js";
import type { GameState, RemotePlayer, ChatMessage } from "./game/types.js";

export type { PlayerMode, FuelType };
export type { GameState, RemotePlayer, ChatMessage } from "./game/types.js";

// ---------------------------------------------------------------------------
// Game component
// ---------------------------------------------------------------------------

interface GameProps {
  playerId?: string;
  physicsSpeed?: number;
  remotePlayers?: RemotePlayer[];
  deposits?: Deposit[];
  inventory?: FuelType[];
  chatMessages?: ChatMessage[];
  onCollectDeposit?: (id: string) => void;
  onRefuel?: (fuelType: FuelType) => void;
  onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
  onBurstDeposit?: (fuelType: string, newX: number) => void;
  onSendMessage?: (text: string) => void;
  onStateChange?: (state: GameState) => void;
}

export function Game({ playerId: externalPlayerId, physicsSpeed, remotePlayers, deposits, inventory, chatMessages, onCollectDeposit, onRefuel, onShareFuel, onBurstDeposit, onSendMessage, onStateChange }: GameProps) {
  const fallbackPlayerId = useRef(getOrCreatePlayerId()).current;
  const playerId = externalPlayerId ?? fallbackPlayerId;
  const playerProps = useRef(derivePlayerProps(playerId)).current;
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const chatInputRef = useRef<HTMLInputElement>(null);

  // Chat state
  const [chatOpen, setChatOpen] = useState(false);
  const chatOpenRef = useRef(false);
  chatOpenRef.current = chatOpen;

  // Map RemotePlayer → RemotePlayerView (staleness already filtered in App.tsx)
  const activeRemotes: RemotePlayerView[] = useMemo(() => {
    if (!remotePlayers) return [];
    return remotePlayers.map((rp) => ({
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
      hasRequiredFuel: rp.hasRequiredFuel,
    }));
  }, [remotePlayers]);

  const engine = useGameEngine(canvasRef, {
    physicsSpeed,
    requiredFuelType: playerProps.requiredFuelType,
    remotePlayers: activeRemotes,
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
    chatOpenRef,
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

  // Chat: all key handling at document level to work reliably in tests.
  // Enter toggles chat (open → send/close, closed → open). Escape closes.
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
      data-lander-y={engine.landerY}
      data-deposit-count={engine.depositCount}
      data-inventory={engine.inventory.join(",")}
      data-remote-player-count={engine.remotePlayerCount}
      data-share-hint={engine.shareHint ? "true" : "false"}
      data-chat-open={chatOpen ? "true" : "false"}
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
      {chatOpen && (
        <input
          ref={chatInputRef}
          data-testid="chat-input"
          type="text"
          maxLength={140}
          autoFocus
          style={{
            position: "absolute",
            bottom: 40,
            left: "50%",
            transform: "translateX(-50%)",
            width: 320,
            padding: "6px 12px",
            fontFamily: "monospace",
            fontSize: 14,
            color: "#00ffff",
            background: "rgba(10, 10, 15, 0.8)",
            border: "1px solid #ff00ff",
            borderRadius: 4,
            outline: "none",
          }}
          placeholder="Type a message..."
        />
      )}
    </div>
  );
}

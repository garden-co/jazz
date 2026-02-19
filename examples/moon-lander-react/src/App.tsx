import type { DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useDb } from "jazz-tools/react";
import { useDeferredValue, useMemo } from "react";
import type { Player } from "../schema/app";
import { app } from "../schema/app";
import { DebugPanel } from "./DebugPanel";
import type { PlayerMode } from "./game/constants";
import { Game } from "./Game";
import { useDeposits } from "./sync/useDeposits";
import { useSyncLoop } from "./sync/useSyncLoop";
import { STALE_THRESHOLD_S } from "./sync/writes";

// ---------------------------------------------------------------------------
// GameWithSync — bridges Game ↔ Jazz DB
// ---------------------------------------------------------------------------

function GameWithSync({
  physicsSpeed,
  initialMode,
  playerId,
}: {
  physicsSpeed?: number;
  initialMode?: PlayerMode;
  playerId: string;
}) {
  const db = useDb();

  // --- Subscribe (edge = wait for server-confirmed data before delivering) ---
  const remotePlayerRows =
    useAll(app.players.where({ playerId: { ne: playerId } }), "edge") ?? [];
  const allChatMessages = useAll(app.chat_messages, "edge") ?? [];
  const depositState = useDeposits(playerId, remotePlayerRows);

  // --- Write ---
  const { gameCallbacks, latestStateRef } = useSyncLoop(
    db,
    playerId,
    depositState,
  );

  // --- View ---
  const remotePlayers: Player[] = useMemo(() => {
    const nowS = Math.floor(Date.now() / 1000);
    return remotePlayerRows.filter(
      (p) => nowS - p.lastSeen < STALE_THRESHOLD_S,
    );
  }, [remotePlayerRows]);

  const chatMessages = useMemo(() => {
    if (!allChatMessages) return [];
    const nowS = Math.floor(Date.now() / 1000);
    return allChatMessages.filter((m) => nowS - m.createdAt < 60);
  }, [allChatMessages]);

  const deferredDeposits = useDeferredValue(depositState.deposits);
  const deferredInventory = useDeferredValue(depositState.inventory);
  const deferredRemotePlayers = useDeferredValue(remotePlayers);
  const deferredChatMessages = useDeferredValue(chatMessages);

  // --- Render ---
  return (
    <>
      <Game
        playerId={playerId}
        physicsSpeed={physicsSpeed}
        initialMode={initialMode}
        remotePlayers={deferredRemotePlayers}
        deposits={deferredDeposits}
        inventory={deferredInventory}
        chatMessages={deferredChatMessages}
        {...gameCallbacks}
      />
      <DebugPanel
        depositState={depositState}
        remotePlayerCount={remotePlayerRows.length}
        chatMessageCount={allChatMessages?.length ?? 0}
        gameState={latestStateRef}
      />
    </>
  );
}

// ---------------------------------------------------------------------------
// App — wraps Game in JazzProvider when config is provided
// ---------------------------------------------------------------------------

interface AppProps {
  config?: DbConfig;
  playerId?: string;
  physicsSpeed?: number;
  initialMode?: PlayerMode;
}

export function App({ config, playerId, physicsSpeed, initialMode }: AppProps) {
  if (!config) {
    return <Game physicsSpeed={physicsSpeed} initialMode={initialMode} />;
  }

  return (
    <JazzProvider config={config}>
      <GameWithSync
        physicsSpeed={physicsSpeed}
        initialMode={initialMode}
        playerId={playerId ?? crypto.randomUUID()}
      />
    </JazzProvider>
  );
}

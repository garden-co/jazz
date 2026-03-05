import { useEffect, useState } from "react";
import type { DbConfig } from "jazz-tools";
import { createJazzClient, JazzProvider } from "jazz-tools/react";
import type { JazzClient } from "jazz-tools/react";
import { DebugPanel } from "./DebugPanel";
import type { PlayerMode } from "./game/constants";
import { Game } from "./Game";
import { useGameSync } from "./sync/useGameSync";

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
  const {
    deposits,
    inventory,
    remotePlayers,
    chatMessages,
    collectDeposit,
    refuel,
    shareFuel,
    burstDeposit,
    sendMessage,
    updateState,
    syncInputs,
    remotePlayerCount,
    chatMessageCount,
    gameState,
  } = useGameSync(playerId);

  return (
    <div
      data-testid="sync-debug"
      data-sync-settled={String(syncInputs.settled)}
      data-sync-local-rows={syncInputs.localPlayerRows.length}
      data-sync-total-deposits={syncInputs.debugTotalDeposits}
    >
      <Game
        playerId={playerId}
        physicsSpeed={physicsSpeed}
        initialMode={initialMode}
        remotePlayers={remotePlayers}
        deposits={deposits}
        inventory={inventory}
        chatMessages={chatMessages}
        onCollectDeposit={collectDeposit}
        onRefuel={refuel}
        onShareFuel={shareFuel}
        onBurstDeposit={burstDeposit}
        onSendMessage={sendMessage}
        onStateChange={updateState}
      />
      <DebugPanel
        syncInputs={syncInputs}
        remotePlayerCount={remotePlayerCount}
        chatMessageCount={chatMessageCount}
        gameState={gameState}
      />
    </div>
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
  const [client, setClient] = useState<JazzClient | null>(null);

  useEffect(() => {
    if (!config) return;

    let active = true;
    const pending = createJazzClient(config);

    void pending.then(
      (resolved) => {
        if (!active) {
          void resolved.shutdown();
          return;
        }
        setClient(resolved);
      },
      (err) => {
        if (!active) return;
        console.error("[moon-lander] Failed to create Jazz client:", err);
      },
    );

    return () => {
      active = false;
      void pending.then((resolved) => resolved.shutdown()).catch(() => {});
    };
  }, [config?.appId, config?.serverUrl, config?.dbName]);

  if (!config) {
    return <Game physicsSpeed={physicsSpeed} initialMode={initialMode} />;
  }

  if (!client) {
    return null;
  }

  return (
    <JazzProvider client={client}>
      <GameWithSync
        physicsSpeed={physicsSpeed}
        initialMode={initialMode}
        playerId={playerId ?? crypto.randomUUID()}
      />
    </JazzProvider>
  );
}

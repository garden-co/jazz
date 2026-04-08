/**
 * GameWithSync — the Jazz-connected version of the Game component.
 *
 * This is the main integration point between Jazz and the game. It:
 *   1. Calls useSync() to subscribe to all Jazz data (players, deposits, chat)
 *   2. Derives game-ready props from the raw subscription results
 *   3. Passes Jazz write callbacks into Game (collect, refuel, share, etc.)
 *   4. Forwards every state change from the game engine back to Jazz via SyncManager
 *
 * Rendered inside <JazzProvider> — see App.tsx.
 */

import { DebugPanel } from "../DebugPanel";
import type { PlayerMode } from "../game/constants";
import { Game } from "../Game";
import { useSync } from "./useSync";

export function GameWithSync({
  physicsSpeed,
  initialMode,
  playerId,
  spawnX,
}: {
  physicsSpeed?: number;
  initialMode?: PlayerMode;
  playerId: string;
  spawnX?: number;
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
  } = useSync(playerId);

  return (
    <div
      data-testid="sync-debug"
      data-sync-settled={String(syncInputs.settled)}
      data-sync-local-rows={syncInputs.localPlayerRows.length}
      data-sync-total-deposits={syncInputs.debugTotalDeposits}
      data-sync-uncollected={syncInputs.uncollectedDeposits.length}
    >
      <Game
        playerId={playerId}
        physicsSpeed={physicsSpeed}
        initialMode={initialMode}
        spawnX={spawnX}
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

import type { DbConfig } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import type { PlayerMode } from "./game/constants";
import { Game } from "./Game";
import { GameWithSync } from "./jazz/GameWithSync";

// ---------------------------------------------------------------------------
// App — wraps Game in JazzProvider when config is provided
// ---------------------------------------------------------------------------

interface AppProps {
  config?: DbConfig;
  playerId?: string;
  physicsSpeed?: number;
  initialMode?: PlayerMode;
  spawnX?: number;
}

export function App({ config, playerId, physicsSpeed, initialMode, spawnX }: AppProps) {
  if (!config) {
    return <Game physicsSpeed={physicsSpeed} initialMode={initialMode} spawnX={spawnX} />;
  }

  return (
    <JazzProvider config={config}>
      <GameWithSync
        physicsSpeed={physicsSpeed}
        initialMode={initialMode}
        playerId={playerId ?? crypto.randomUUID()}
        spawnX={spawnX}
      />
    </JazzProvider>
  );
}

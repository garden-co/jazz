import { Suspense, useEffect, useMemo } from "react";
import type { DbConfig } from "jazz-tools";
import { createJazzClient, JazzProvider } from "jazz-tools/react";
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
    <ConnectedApp
      config={config}
      playerId={playerId}
      physicsSpeed={physicsSpeed}
      initialMode={initialMode}
      spawnX={spawnX}
    />
  );
}

// Commits immediately; suspends inside JazzProvider until the client is ready.
// JazzProvider (via CoreJazzProvider) calls use() internally on the promise.
function ConnectedApp({
  config,
  playerId,
  physicsSpeed,
  initialMode,
  spawnX,
}: AppProps & { config: DbConfig }) {
  const clientPromise = useMemo(
    () => createJazzClient(config),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [config.appId, config.serverUrl, config.dbName],
  );

  useEffect(
    () => () => {
      clientPromise.then((c) => c.shutdown());
    },
    [clientPromise],
  );

  return (
    <Suspense>
      <JazzProvider client={clientPromise}>
        <GameWithSync
          physicsSpeed={physicsSpeed}
          initialMode={initialMode}
          playerId={playerId ?? crypto.randomUUID()}
          spawnX={spawnX}
        />
      </JazzProvider>
    </Suspense>
  );
}

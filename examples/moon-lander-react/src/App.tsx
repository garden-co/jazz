import { useEffect, useState } from "react";
import type { DbConfig } from "jazz-tools";
import { createJazzClient, JazzProvider } from "jazz-tools/react";
import type { JazzClient } from "jazz-tools/react";
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
  const [client, setClient] = useState<JazzClient | null>(null);
  const [error, setError] = useState<unknown>(null);

  useEffect(() => {
    if (!config) return;

    let active = true;
    const client = createJazzClient(config);

    client.then(
      (resolved) => {
        if (!active) {
          resolved.shutdown();
          return;
        }
        setClient(resolved);
      },
      (reason) => {
        if (!active) return;
        setError(reason);
      },
    );

    return () => {
      active = false;
      client.then((resolved) => resolved.shutdown()).catch(() => {});
    };
  }, [config?.appId, config?.serverUrl, config?.dbName]);

  if (error) {
    throw error;
  }

  if (!config) {
    return <Game physicsSpeed={physicsSpeed} initialMode={initialMode} spawnX={spawnX} />;
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
        spawnX={spawnX}
      />
    </JazzProvider>
  );
}

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
  useEffect(() => {
    if (!config) return;

    let active = true;
    let jazzClient: JazzClient | null = null;

    createJazzClient(config).then(
      (resolved) => {
        if (!active) {
          resolved.shutdown();
          return;
        }
        jazzClient = resolved;
        setClient(resolved);
      },
      (err) => {
        if (!active) return;
        console.error("[moon-lander] Failed to create Jazz client:", err);
      },
    );

    return () => {
      active = false;
      jazzClient?.shutdown();
    };
  }, [config?.appId, config?.serverUrl, config?.dbName]);

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

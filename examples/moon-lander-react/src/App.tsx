import type { DbConfig } from "jazz-tools";
import { JazzProvider, JazzSessionProvider } from "jazz-tools/react";
import type { PlayerMode } from "./game/constants";
import { Game } from "./Game";
import { GameWithSync } from "./jazz/GameWithSync";

// ---------------------------------------------------------------------------
// App — wraps Game in a Jazz provider when config is provided
// ---------------------------------------------------------------------------

/** Session config, optionally pinned to an already prepared account handle. */
export type AppConfig = Omit<DbConfig, "account"> & {
  serverUrl: string;
  account?: DbConfig["account"];
};

interface AppProps {
  config?: AppConfig;
  playerId?: string;
  physicsSpeed?: number;
  initialMode?: PlayerMode;
  spawnX?: number;
}

export function App({ config, playerId, physicsSpeed, initialMode, spawnX }: AppProps) {
  if (!config) {
    return <Game physicsSpeed={physicsSpeed} initialMode={initialMode} spawnX={spawnX} />;
  }

  const game = (
    <GameWithSync
      physicsSpeed={physicsSpeed}
      initialMode={initialMode}
      playerId={playerId ?? crypto.randomUUID()}
      spawnX={spawnX}
    />
  );

  // Callers that restore a specific identity (tests, recovery secrets) prepare
  // the account handle themselves; everyone else gets a local-first account
  // that the session creates on first load and restores from localStorage.
  const { account, ...sessionConfig } = config;
  if (account) {
    return <JazzProvider config={{ ...sessionConfig, account }}>{game}</JazzProvider>;
  }
  return (
    <JazzSessionProvider config={{ env: "dev", ...sessionConfig, initial: "local-first" }}>
      {game}
    </JazzSessionProvider>
  );
}

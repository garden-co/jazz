import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import { getOrCreatePlayerId } from "./game/player.js";

// ---------------------------------------------------------------------------
// Dev-mode Jazz server config (server started automatically by `pnpm dev`)
// ---------------------------------------------------------------------------

const DEV_SERVER_PORT = 4200;
const DEV_APP_ID = "00000000-0000-0000-0000-000000000005";

function main() {
  const serverUrl = `http://127.0.0.1:${DEV_SERVER_PORT}`;

  console.info("[moon-lander] Connecting to Jazz server at %s", serverUrl);

  // Stable identity from localStorage. Each browser context (or Firefox
  // container) is a separate player, consistent across refreshes.
  const playerId = getOrCreatePlayerId();

  // Stable dbName per tab — reusing the same OPFS database across refreshes
  // means the local player row and deposits persist, avoiding ghost duplicates.
  const dbName = `moon-lander-${playerId.slice(0, 8)}`;

  createRoot(document.getElementById("root")!).render(
    <App
      playerId={playerId}
      config={{
        appId: DEV_APP_ID,
        dbName,
        serverUrl,
      }}
    />,
  );
}

main();

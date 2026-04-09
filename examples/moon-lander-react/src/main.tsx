import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import { getOrCreatePlayerId } from "./game/player.js";

// ---------------------------------------------------------------------------
// Dev-mode Jazz server config (server started automatically by `pnpm dev`)
// ---------------------------------------------------------------------------

const DEV_SERVER_PORT = 4200;
const DEV_APP_ID = "00000000-0000-0000-0000-000000000005";

function main() {
  const params = new URLSearchParams(window.location.search);

  // URL search params override dev defaults (used by isolated browser test
  // contexts that navigate to index.html with config params).
  const appId = params.get("appId") ?? DEV_APP_ID;
  const serverUrl = params.get("serverUrl") ?? `http://127.0.0.1:${DEV_SERVER_PORT}`;
  const playerId = params.get("playerId") ?? getOrCreatePlayerId();
  const physicsSpeed = params.has("physicsSpeed") ? Number(params.get("physicsSpeed")) : undefined;
  const spawnX = params.has("spawnX") ? Number(params.get("spawnX")) : undefined;

  // Stable dbName per tab — reusing the same OPFS database across refreshes
  // means the local player row and deposits persist, avoiding ghost duplicates.
  const dbName = params.get("dbName") ?? `moon-lander-${playerId.slice(0, 8)}`;

  const jwtToken = params.get("jwtToken") ?? undefined;
  const adminSecret = params.get("adminSecret") ?? undefined;

  console.info(
    "[moon-lander] Connecting to Jazz server at %s (token=%s, admin=%s)",
    serverUrl,
    jwtToken ? "yes" : "auto",
    adminSecret ? "yes" : "no",
  );

  createRoot(document.getElementById("root")!).render(
    <App
      playerId={playerId}
      physicsSpeed={physicsSpeed}
      initialMode={params.has("appId") ? "landed" : undefined}
      {...(spawnX !== undefined ? { spawnX } : {})}
      config={{
        appId,
        dbName,
        serverUrl,
        ...(jwtToken ? { jwtToken } : {}),
        ...(adminSecret ? { adminSecret } : {}),
      }}
    />,
  );
}

main();

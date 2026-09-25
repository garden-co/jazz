import { createAccountManager } from "jazz-tools";
import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import { getOrCreatePlayerId } from "./game/player.js";

async function main() {
  const params = new URLSearchParams(window.location.search);

  // URL search params override plugin-injected defaults (used by isolated
  // browser test contexts that navigate to index.html with config params).
  const appId = params.get("appId") ?? import.meta.env.VITE_JAZZ_APP_ID;
  const serverUrl = params.get("serverUrl") ?? import.meta.env.VITE_JAZZ_SERVER_URL;
  const playerId = params.get("playerId") ?? getOrCreatePlayerId();
  const physicsSpeed = params.has("physicsSpeed") ? Number(params.get("physicsSpeed")) : undefined;
  const spawnX = params.has("spawnX") ? Number(params.get("spawnX")) : undefined;

  // Stable dbName per tab — reusing the same IndexedDB database across refreshes
  // means the local player row and deposits persist, avoiding ghost duplicates.
  const dbName = params.get("dbName") ?? `moon-lander-${playerId.slice(0, 8)}`;

  const localFirstSecret = params.get("localFirstSecret") ?? undefined;
  const adminSecret = params.get("adminSecret") ?? undefined;

  console.info(
    "[moon-lander] Connecting to Jazz server at %s (secret=%s, admin=%s)",
    serverUrl,
    localFirstSecret ? "yes" : "auto",
    adminSecret ? "yes" : "no",
  );

  if (!appId) throw new Error("Missing Jazz appId (VITE_JAZZ_APP_ID or ?appId=)");
  if (!serverUrl) throw new Error("Missing Jazz serverUrl (VITE_JAZZ_SERVER_URL or ?serverUrl=)");

  // A recovery secret pins this tab to a known local-first account; otherwise
  // the session provider creates one and restores it on later visits.
  const account = localFirstSecret
    ? (await createAccountManager({ appId, serverUrl })).restoreLocalFirst(localFirstSecret)
    : undefined;

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
        ...(account ? { account } : {}),
        ...(adminSecret ? { adminSecret } : {}),
      }}
    />,
  );
}

void main();

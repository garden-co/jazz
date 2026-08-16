import { createRoot } from "react-dom/client";
import { BrowserAuthSecretStore } from "jazz-tools";
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

  // Stable dbName per tab — reusing the same OPFS database across refreshes
  // means the local player row and deposits persist, avoiding ghost duplicates.
  const dbName = params.get("dbName") ?? `moon-lander-${playerId.slice(0, 8)}`;

  const adminSecret = params.get("adminSecret") ?? undefined;
  const localFirstSecret =
    params.get("localFirstSecret") ??
    (adminSecret ? undefined : await BrowserAuthSecretStore.getOrCreateSecret({ appId }));

  console.info(
    "[moon-lander] Connecting to Jazz server at %s (secret=%s, admin=%s)",
    serverUrl,
    localFirstSecret ? "yes" : "auto",
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
        ...(localFirstSecret ? { secret: localFirstSecret } : {}),
        ...(adminSecret ? { adminSecret } : {}),
      }}
    />,
  );
}

void main().catch((error) => {
  console.error("[moon-lander] Failed to start", error);
});

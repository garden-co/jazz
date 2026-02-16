import { createRoot } from "react-dom/client";
import { App } from "./App.js";

// ---------------------------------------------------------------------------
// Dev-mode Jazz server config
// ---------------------------------------------------------------------------
// Start the server with: pnpm dev:server (in a separate terminal)

const DEV_SERVER_PORT = 4200;
const DEV_JWT_SECRET = "dev-jwt-secret-moon-lander";
const DEV_ADMIN_SECRET = "dev-admin-secret-moon-lander";
const DEV_APP_ID = "00000000-0000-0000-0000-000000000003";

function base64url(input: string | Uint8Array): string {
  const str =
    typeof input === "string"
      ? btoa(input)
      : btoa(String.fromCharCode(...input));
  return str.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function signJwt(sub: string, secret: string): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    sub,
    claims: {},
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const enc = new TextEncoder();
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const data = enc.encode(`${headerB64}.${payloadB64}`);
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, data);
  return `${headerB64}.${payloadB64}.${base64url(new Uint8Array(sig))}`;
}

async function main() {
  const serverUrl = `http://127.0.0.1:${DEV_SERVER_PORT}`;

  // Always attempt to connect — JazzProvider handles server unavailability
  // gracefully via reconnect backoff. Log a hint for developers.
  console.info("[moon-lander] Connecting to Jazz server at %s", serverUrl);

  // Each tab is a separate player. sessionStorage is per-tab (unique across
  // tabs) but survives page refreshes within the same tab, so a refresh
  // reconnects as the same Jazz player row rather than creating a new one.
  // Visual identity (name, colour) is derived from localStorage in Game.tsx.
  const KEY = "moon-lander-session-id";
  let playerId = sessionStorage.getItem(KEY);
  if (!playerId) {
    playerId = crypto.randomUUID();
    sessionStorage.setItem(KEY, playerId);
  }

  // Stable dbName per tab — reusing the same OPFS database across refreshes
  // means the local player row and deposits persist, avoiding ghost duplicates.
  const dbName = `moon-lander-${playerId.slice(0, 8)}`;
  const jwtToken = await signJwt(playerId, DEV_JWT_SECRET);

  createRoot(document.getElementById("root")!).render(
    <App
      playerId={playerId}
      config={{
        appId: DEV_APP_ID,
        dbName,
        serverUrl,
        jwtToken,
        adminSecret: DEV_ADMIN_SECRET,
      }}
    />,
  );
}

main();

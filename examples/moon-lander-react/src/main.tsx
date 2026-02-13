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

  // Check if the Jazz server is running
  let serverAvailable = false;
  try {
    const resp = await fetch(`${serverUrl}/health`);
    serverAvailable = resp.ok;
  } catch {
    // Server not running
  }

  if (!serverAvailable) {
    console.warn(
      "[moon-lander] Jazz server not running at %s — starting without sync.\n" +
        "Run `pnpm dev:server` in a separate terminal for multiplayer.",
      serverUrl,
    );
    createRoot(document.getElementById("root")!).render(<App />);
    return;
  }

  // Each tab gets a unique OPFS database name to avoid lock conflicts.
  // The player ID (from localStorage) is stable across tabs.
  const KEY = "moon-lander-player-id";
  let playerId = localStorage.getItem(KEY);
  if (!playerId) {
    playerId = crypto.randomUUID();
    localStorage.setItem(KEY, playerId);
  }

  const dbName = `moon-lander-${playerId.slice(0, 8)}-${Date.now()}`;
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

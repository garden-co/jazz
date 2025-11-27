import { SyncConfig } from "jazz-tools";
import { JazzReactProvider } from "jazz-tools/react";

const url = new URL(window.location.href);

const key = `${getUserInfo()}@jazz.tools`;

let peer =
  (url.searchParams.get("peer") as `ws://${string}`) ??
  `wss://cloud.jazz.tools/?key=${key}`;

if (url.searchParams.has("local")) {
  peer = `ws://localhost:4200/?key=${key}`;
}

if (import.meta.env.VITE_WS_PEER) {
  peer = import.meta.env.VITE_WS_PEER;
}

function getUserInfo() {
  return url.searchParams.get("userName") ?? "Mister X";
}

function getSyncConfig(): SyncConfig {
  const syncWhen = url.searchParams.get("syncWhen") ?? "always";

  return {
    peer: `${peer}?key=${key}`,
    when: syncWhen as "always" | "signedUp" | "never",
  };
}

export function AuthAndJazz({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider sync={getSyncConfig()}>{children}</JazzReactProvider>
  );
}

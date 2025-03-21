import { JazzProvider } from "jazz-react";

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

export function AuthAndJazz({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: `${peer}?key=${key}`,
      }}
    >
      {children as any}
    </JazzProvider>
  );
}

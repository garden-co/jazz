"use client";

import { JazzProvider as Jazz } from "jazz-tools/react";
import { StatusScreen } from "./status-screen";

// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes) in its own chunk once the page has loaded.
const loadLocalFirst = () => import("jazz-tools/local-first");

// Better Auth signs users in; the app's own Jazz (`createJazz` in
// src/lib/backend-jazz-client.ts) reads its session in `authenticate`.
export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <Jazz
      serverUrl="/jazz"
      client={loadLocalFirst}
      // Signed out, the app renders its own sign-in screen without Jazz.
      signedOut={children}
      loading={<StatusScreen message="Opening MusicAgent…" />}
      error={(state) => (
        <StatusScreen error={state.error?.message ?? "Could not open MusicAgent."} />
      )}
    >
      {children}
    </Jazz>
  );
}

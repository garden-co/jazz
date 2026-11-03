"use client";

import { JazzProvider as Jazz, betterAuth } from "jazz-tools/react";
import { authClient } from "@/src/lib/auth-client";
import { jazzAppId } from "@/src/lib/jazz-env";
import { StatusScreen } from "./status-screen";

// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes) in its own chunk once the page has loaded.
const loadLocalFirst = () => import("jazz-tools/local-first");

// Better Auth's session drives sign-in; the app's own Jazz (`createJazz` in
// src/lib/backend-jazz-client.ts) reads it in `authenticate`.
const auth = betterAuth(authClient);

export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <Jazz
      appId={jazzAppId}
      serverUrl="/jazz"
      client={loadLocalFirst}
      auth={auth}
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

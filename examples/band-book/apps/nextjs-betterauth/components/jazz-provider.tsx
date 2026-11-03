"use client";

import { Button } from "@astryxdesign/core";
import { JazzProvider as CoreJazzProvider } from "jazz-tools/react";
import { SignInForm } from "@/components/sign-in-form";
import { StatusScreen } from "@/components/status-screen";
import { jazzAppId } from "@/src/lib/config";

// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes) in its own chunk once the page has loaded.
const loadLocalFirst = () => import("jazz-tools/local-first");

/**
 * Jazz owns the session lifecycle: it asks BandBook's backend (`createJazz` in
 * src/lib/jazz.server.ts) who is signed in, which reads Better Auth's session
 * in `authenticate`, and renders the app only once
 * its client is ready. Signed-out visitors see the sign-in form wherever they
 * land, so an invite link works before and after signing in.
 */
export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <CoreJazzProvider
      appId={jazzAppId}
      serverUrl="/jazz"
      client={loadLocalFirst}
      signedOut={<SignInForm />}
      loading={<StatusScreen label="Opening BandBook" />}
      error={(state) => (
        <StatusScreen
          label="Could not open BandBook"
          error={state.error?.message ?? "Something went wrong."}
          action={<Button label="Try again" onClick={() => void state.retry()} />}
        />
      )}
    >
      {children}
    </CoreJazzProvider>
  );
}

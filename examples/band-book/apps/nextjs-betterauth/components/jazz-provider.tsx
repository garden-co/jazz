"use client";

import { Button } from "@astryxdesign/core";
import { betterAuth, JazzProvider as CoreJazzProvider } from "jazz-tools/react";
import { SignInForm } from "@/components/sign-in-form";
import { StatusScreen } from "@/components/status-screen";
import { authClient } from "@/src/lib/auth-client";
import { jazzAppId, jazzEnv, jazzServerUrl } from "@/src/lib/config";

const auth = betterAuth(authClient);

/**
 * Jazz owns the session lifecycle: it follows Better Auth's session,
 * atomically logs in or registers the account, and renders the app only once
 * its client is ready. Signed-out visitors see the sign-in form wherever they
 * land, so an invite link works before and after signing in.
 */
export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <CoreJazzProvider
      appId={jazzAppId}
      serverUrl={jazzServerUrl}
      env={jazzEnv}
      auth={auth}
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

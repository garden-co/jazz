"use client";

import { useEffect } from "react";
import { JazzProvider as Jazz, betterAuth, useJazzAuth, useSession } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
import { ErrorScreen, LoadingScreen } from "@/components/status-screen";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID!;
// Better Auth's session drives sign-in; Wequencer's backend (`createJazz` in
// lib/jazz.server.ts) reads it in `authenticate`. Signing up creates the
// bandmate's Jazz account, signing in opens it again.
const auth = betterAuth(authClient);

const profileRequests = new Map<string, Promise<void>>();
const profileKey = (accountId: string) => `wequencer-profile:${accountId}`;

/**
 * Asks the server to create (or rename) the account's profile, at most once
 * per account and display name: the name is remembered in localStorage, and
 * concurrent callers (StrictMode, remounts) share one request. Failures are
 * logged and retried on the next load.
 */
function ensureProfileOnce(accountId: string, displayName: string) {
  try {
    if (localStorage.getItem(profileKey(accountId)) === displayName) return;
  } catch {
    // Storage unavailable: ask the server, which is idempotent.
  }
  const requestKey = `${accountId}:${displayName}`;
  if (profileRequests.has(requestKey)) return;
  const request = (async () => {
    // The session cookie is the only credential the server needs.
    const response = await fetch("/api/bootstrap", { method: "POST", credentials: "same-origin" });
    if (!response.ok) throw new Error(`Profile bootstrap failed (${response.status})`);
    try {
      localStorage.setItem(profileKey(accountId), displayName);
    } catch {}
  })();
  profileRequests.set(requestKey, request);
  request.catch((cause) => {
    profileRequests.delete(requestKey);
    // Console only, on purpose. The profile is just the display name bandmates
    // see beside the presence avatars; without it they see "Bandmate" and
    // everything else (the account, the grid, sync) works as before. The
    // server call is idempotent and nothing was remembered, so the next load
    // asks again. An error screen or banner would interrupt a working session
    // over a cosmetic, self-healing failure.
    console.error("Wequencer profile setup failed", cause);
  });
}

/** The profile only labels presence avatars: create it in the background. */
function ProfileSetup() {
  const account = useSession()?.user.account;
  const { data } = authClient.useSession();
  const displayName = data?.user.name;
  useEffect(() => {
    if (account && displayName) ensureProfileOnce(account, displayName);
  }, [account, displayName]);
  return null;
}

/** Signs out of Jazz first (flushing local writes), then of Better Auth. */
export function useGracefulSignOut() {
  return useJazzAuth().logout;
}

export function JazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <Jazz
      appId={APP_ID}
      serverUrl="/jazz"
      auth={auth}
      // Signed-out pages (the sign-in form) need no Jazz.
      signedOut={children}
      loading={<LoadingScreen label="Opening your Jazz account…" />}
      error={(state) => (
        <ErrorScreen
          message={state.error?.message ?? "Unknown error"}
          onRetry={() => void state.retry()}
        />
      )}
    >
      <ProfileSetup />
      {children}
    </Jazz>
  );
}

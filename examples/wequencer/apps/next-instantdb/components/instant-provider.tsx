"use client";

import { useEffect } from "react";
import { authClient } from "@/lib/auth-client";

// The app's server (app/api/auth) signs people in and mints Instant tokens;
// the browser then talks to Instant directly.

const profileRequests = new Map<string, Promise<void>>();
const profileKey = (accountId: string) => `wequencer-profile:${accountId}`;

/**
 * Asks the server to create (or rename) the account's profile, at most once
 * per account and display name: the name is remembered in localStorage, and
 * concurrent callers (StrictMode, remounts) share one request. Failures are
 * logged and retried on the next load.
 */
function ensureProfileOnce(accountId: string, displayName: string, token: string) {
  try {
    if (localStorage.getItem(profileKey(accountId)) === displayName) return;
  } catch {
    // Storage unavailable: ask the server, which is idempotent.
  }
  const requestKey = `${accountId}:${displayName}`;
  if (profileRequests.has(requestKey)) return;
  const request = (async () => {
    // The Instant token is the only credential the server needs.
    const response = await fetch("/api/bootstrap", {
      method: "POST",
      headers: { authorization: `Bearer ${token}` },
    });
    if (!response.ok) throw new Error(`Profile bootstrap failed (${response.status})`);
    try {
      localStorage.setItem(profileKey(accountId), displayName);
    } catch {}
  })();
  profileRequests.set(requestKey, request);
  request.catch((cause) => {
    profileRequests.delete(requestKey);
    // Console only, on purpose: the profile is just the display name bandmates
    // see beside the presence avatars, and the next load asks again.
    console.error("Wequencer profile setup failed", cause);
  });
}

/** The profile only labels presence avatars: create it in the background. */
function ProfileSetup() {
  const { data } = authClient.useSession();
  const user = data?.user;
  useEffect(() => {
    if (user?.name && user.refreshToken) ensureProfileOnce(user.id, user.name, user.refreshToken);
  }, [user?.id, user?.name, user?.refreshToken]);
  return null;
}

/** Signs out of Instant. */
export function useGracefulSignOut() {
  return () => authClient.signOut();
}

export function InstantProvider({ children }: { children: React.ReactNode }) {
  return (
    <>
      <ProfileSetup />
      {children}
    </>
  );
}

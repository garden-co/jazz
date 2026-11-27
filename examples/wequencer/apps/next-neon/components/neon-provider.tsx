"use client";

import { useEffect, useMemo } from "react";
import { authClient } from "@/lib/auth-client";
import { DbContext } from "@/lib/db";
import { writer } from "@/lib/live";

// Managed Better Auth's session cookie (on the app's origin) drives sign-in;
// the sealing route and the write Server Action read it. Live queries need no
// provider: they share one Realtime client.

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
    // Console only: the profile just labels presence avatars, and the next
    // load asks again.
    console.error("Wequencer profile setup failed", cause);
  });
}

/** The profile only labels presence avatars: create it in the background. */
function ProfileSetup({ account, displayName }: { account: string; displayName: string }) {
  useEffect(() => {
    if (account && displayName) ensureProfileOnce(account, displayName);
  }, [account, displayName]);
  return null;
}

/** Signs out of Managed Better Auth; the caller then reloads, which drops the live queries. */
export function useGracefulSignOut() {
  return async () => {
    await authClient.signOut();
  };
}

export function NeonProvider({
  user,
  children,
}: {
  /** The signed-in Managed Better Auth user, read on the server; null when signed out. */
  user: { id: string; name: string } | null;
  children: React.ReactNode;
}) {
  const db = useMemo(() => (user ? writer(user.id) : null), [user?.id]);
  // Signed-out pages (the sign-in form) need no writes.
  if (!user) return <>{children}</>;
  return (
    <DbContext.Provider value={db}>
      <ProfileSetup account={user.id} displayName={user.name} />
      {children}
    </DbContext.Provider>
  );
}

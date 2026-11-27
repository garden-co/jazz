"use client";

import { createContext, useContext, useEffect, useMemo } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import { authClient } from "@/lib/auth-client";
import { connectLive, queryClient } from "@/lib/data/live";
import { db } from "@/mutators";

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

/** Signs out of Better Auth; the caller then reloads, which drops the cache. */
export function useGracefulSignOut() {
  return async () => {
    await authClient.signOut();
  };
}

const UserContext = createContext<string | undefined>(undefined);

/** The writes, and the signed-in account as `userID`. */
export function useDb() {
  const userID = useContext(UserContext);
  return useMemo(() => ({ userID, mutate: db.mutate }), [userID]);
}
export type Db = ReturnType<typeof useDb>;

/** The data layer for the signed-in Better Auth user: the query cache and the live socket. */
export function DataProvider({
  user,
  children,
}: {
  /** The signed-in Better Auth user, read on the server; null when signed out. */
  user: { id: string; name: string } | null;
  children: React.ReactNode;
}) {
  useEffect(() => {
    if (user) connectLive();
  }, [user]);
  // Signed-out pages (the sign-in form) need no data layer.
  if (!user) return <>{children}</>;
  return (
    <QueryClientProvider client={queryClient}>
      <UserContext.Provider value={user.id}>
        <ProfileSetup account={user.id} displayName={user.name} />
        {children}
      </UserContext.Provider>
    </QueryClientProvider>
  );
}

"use client";

import { useEffect } from "react";
import { ZeroProvider as Zero } from "@rocicorp/zero/react";
import { authClient } from "@/lib/auth-client";
import { mutators } from "@/mutators";
import { schema } from "@/schema";

// Better Auth's session drives sign-in; zero-cache forwards its cookie to
// the app's /api/zero endpoints, which read it. zero-cache runs next to the
// app on the page's own host, so the browser sends it the cookie.
// (Server rendering has no `location`; the app's origin names the same host.)
const cacheURL = () => {
  const page = typeof location === "undefined" ? new URL(process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1") : location;
  return `${page.protocol}//${page.hostname}:${process.env.NEXT_PUBLIC_ZERO_CACHE_PORT ?? "4848"}`;
};

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

/** Signs out of Better Auth; the caller then reloads, which drops Zero. */
export function useGracefulSignOut() {
  return async () => {
    await authClient.signOut();
  };
}

export function ZeroProvider({
  user,
  children,
}: {
  /** The signed-in Better Auth user, read on the server; null when signed out. */
  user: { id: string; name: string } | null;
  children: React.ReactNode;
}) {
  // Signed-out pages (the sign-in form) need no Zero.
  if (!user) return <>{children}</>;
  return (
    <Zero userID={user.id} context={{ userID: user.id }} cacheURL={cacheURL()} schema={schema} mutators={mutators}>
      <ProfileSetup account={user.id} displayName={user.name} />
      {children}
    </Zero>
  );
}

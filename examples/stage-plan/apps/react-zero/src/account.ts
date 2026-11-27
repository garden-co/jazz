import { useEffect, useState } from "react";
import { decodeJwt } from "jose";

/**
 * An anonymous account, like the Jazz device account: no sign-up. The app's
 * server issues it as a JWT in a cookie on first visit; zero-cache forwards
 * the cookie to the server's query and mutate endpoints.
 */
function accountFromCookie(): string | undefined {
  const jwt = document.cookie.match(/(?:^|;\s*)jwt=([^;]+)/)?.[1];
  if (!jwt) return undefined;
  try {
    const { sub, exp } = decodeJwt(jwt);
    return typeof sub === "string" && (!exp || exp * 1000 > Date.now()) ? sub : undefined;
  } catch {
    return undefined;
  }
}

/** The signed-in account, signing in anonymously first if needed. */
export function useAccount(): string | undefined {
  const [account, setAccount] = useState(accountFromCookie);
  useEffect(() => {
    if (account) return;
    fetch("/api/login", { credentials: "include" })
      .then((response) => response.json())
      .then(({ userID }: { userID: string }) => setAccount(userID));
  }, [account]);
  return account;
}

/** zero-cache runs next to the app, on the page's own host (so it gets the cookie). */
export const cacheURL = `${location.protocol}//${location.hostname}:${import.meta.env.VITE_ZERO_CACHE_PORT ?? "4848"}`;

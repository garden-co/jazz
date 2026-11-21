import { createAuthClient } from "@neon/auth";
import { BetterAuthReactAdapter } from "@neon/auth/react/adapters";
import { setAuthHeaders } from "./live";

/** Managed Better Auth, straight from the browser (its URL is public). */
export const authClient = createAuthClient(import.meta.env.VITE_NEON_AUTH_URL, {
  adapter: BetterAuthReactAdapter(),
});

// The app's server takes the session's JWT as a bearer token; getSession()
// returns the cached one until it's about to expire.
setAuthHeaders(async (): Promise<Record<string, string>> => {
  const { data } = await authClient.getSession();
  const token = data?.session.token;
  return token ? { Authorization: `Bearer ${token}` } : {};
});

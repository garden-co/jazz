import { ZeroProvider as Zero } from "@rocicorp/zero/react";
import { authClient } from "@/src/lib/auth-client";
import { mutators } from "@/mutators";
import { schema } from "@/schema";

// zero-cache runs next to the app on the page's own host, so the browser sends
// it Better Auth's session cookie, which it forwards to the app's endpoints.
const cacheURL = `${location.protocol}//${location.hostname}:${import.meta.env.VITE_ZERO_CACHE_PORT ?? "4848"}`;

type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  return {
    async signOut() {
      await authClient.signOut();
      window.location.assign("/");
    },
  };
}

/** Zero for the signed-in Better Auth user. */
export function ZeroProvider({ userID, children }: React.PropsWithChildren<{ userID: string }>) {
  return (
    <Zero userID={userID} context={{ userID }} cacheURL={cacheURL} schema={schema} mutators={mutators}>
      {children}
    </Zero>
  );
}

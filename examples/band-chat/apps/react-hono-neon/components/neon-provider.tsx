import { useMemo } from "react";
import { authClient } from "@/src/lib/auth-client";
import { DbContext } from "@/src/lib/db";
import { writer } from "@/src/lib/live";

type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  return {
    async signOut() {
      await authClient.signOut();
      window.location.assign("/");
    },
  };
}

/** The signed-in Managed Better Auth user's writes; live queries need no provider. */
export function NeonProvider({ userID, children }: React.PropsWithChildren<{ userID: string }>) {
  const db = useMemo(() => writer(userID), [userID]);
  return <DbContext.Provider value={db}>{children}</DbContext.Provider>;
}

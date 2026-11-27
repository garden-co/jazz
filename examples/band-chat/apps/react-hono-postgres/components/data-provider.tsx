import { useEffect } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import { authClient } from "@/src/lib/auth-client";
import { connectLive, queryClient } from "@/src/data/live";

type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  return {
    async signOut() {
      await authClient.signOut();
      window.location.assign("/");
    },
  };
}

/** The data layer for the signed-in Better Auth user: the query cache and the live socket. */
export function DataProvider({ children }: React.PropsWithChildren) {
  useEffect(() => connectLive(), []);
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

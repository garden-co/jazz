import { ConvexAuthProvider, useAuthActions } from "@convex-dev/auth/react";
import { convex } from "@/src/convex";

// Convex Auth signs users in with email and password; every query and
// mutation then runs as that user (`getAuthUserId` in the functions).
type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  const { signOut } = useAuthActions();
  return {
    async signOut() {
      await signOut();
      window.location.assign("/");
    },
  };
}

export function ConvexProvider({ children }: React.PropsWithChildren) {
  return <ConvexAuthProvider client={convex}>{children}</ConvexAuthProvider>;
}

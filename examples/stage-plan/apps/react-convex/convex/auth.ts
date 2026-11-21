import { convexAuth } from "@convex-dev/auth/server";
import { Anonymous } from "@convex-dev/auth/providers/Anonymous";

// No sign-up: every browser signs in as an anonymous guest, like Jazz's device accounts.
export const { auth, signIn, signOut, store, isAuthenticated } = convexAuth({
  providers: [Anonymous],
});

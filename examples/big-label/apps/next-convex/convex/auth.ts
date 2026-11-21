import { convexAuth } from "@convex-dev/auth/server";
import { Password } from "@convex-dev/auth/providers/Password";

// Email and password, like the Jazz app's Better Auth setup (8+ characters).
export const { auth, signIn, signOut, store, isAuthenticated } = convexAuth({
  providers: [
    Password({
      profile: (params) => ({
        email: String(params.email).trim().toLowerCase(),
        name: String(params.name ?? ""),
      }),
      validatePasswordRequirements: (password) => {
        if (password.length < 8) throw new Error("Password must be at least 8 characters.");
      },
    }),
  ],
});

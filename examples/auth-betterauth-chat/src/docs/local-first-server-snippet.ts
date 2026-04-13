import { betterAuth } from "better-auth";
import { APIError, createAuthMiddleware } from "better-auth/api";
import { jwt } from "better-auth/plugins";

// #region local-first-verify-hook
export const auth = betterAuth({
  // ...your database, email, plugins config
  plugins: [
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: "https://your-app.example.com",
        definePayload: ({ user }) => ({
          claims: { role: (user as { role?: string }).role ?? "" },
        }),
      },
    }),
  ],
  hooks: {
    before: createAuthMiddleware(async (ctx) => {
      if (ctx.path !== "/sign-up/email") return;

      const proofToken = ctx.body?.proofToken;
      if (!proofToken) {
        throw new APIError("BAD_REQUEST", {
          message: "proofToken is required for sign-up",
        });
      }

      // Verify the proof token using Jazz's NAPI binding
      const { verifyLocalFirstIdentityProof } = await import("jazz-napi");
      const { ok, id: provedUserId } = verifyLocalFirstIdentityProof(
        proofToken,
        "betterauth-signup",
      );
      if (!ok) {
        throw new APIError("UNAUTHORIZED", {
          message: "Invalid proof token",
        });
      }

      return {
        context: {
          ...ctx,
          body: { ...ctx.body, provedUserId },
        },
      };
    }),
  },
  databaseHooks: {
    user: {
      create: {
        before: async (user: any, ctx: any) => {
          // Assign the proven Jazz user ID to the BetterAuth user
          const provedUserId = ctx?.body?.provedUserId;
          if (provedUserId) {
            return { data: { ...user, id: provedUserId } };
          }
        },
      },
    },
  },
});
// #endregion local-first-verify-hook

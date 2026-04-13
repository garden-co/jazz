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

      const { verifyLocalFirstIdentityProof } = await import("jazz-napi");
      const {
        ok,
        error,
        id: provedUserId,
      } = verifyLocalFirstIdentityProof(ctx.body?.proofToken, "betterauth-signup");
      if (!ok) {
        throw new APIError("BAD_REQUEST", { message: error });
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

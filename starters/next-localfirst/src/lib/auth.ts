import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { APIError, createAuthMiddleware } from "better-auth/api";

const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3000";

if (!process.env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Run 'pnpm install' to generate .env.local, or set it explicitly in your environment.",
  );
}

const BETTER_AUTH_SECRET = process.env.BETTER_AUTH_SECRET;

// In-memory store. Each Node process keeps its own copy, so users that sign
// up in one worker cannot sign in from another: HMR reloads, multi-worker
// deploys, and serverless invocations all reset state. Good enough for a
// starter you run locally; swap for a persistent adapter before you ship.
const authMemoryDb: MemoryDB = {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
};

export const auth = betterAuth({
  baseURL: APP_ORIGIN,
  secret: BETTER_AUTH_SECRET,
  database: memoryAdapter(authMemoryDb),
  trustedOrigins: [APP_ORIGIN],
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    // Industry-standard minimum; tune to whatever your product requires.
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  hooks: {
    before: createAuthMiddleware(async (ctx) => {
      if (ctx.path !== "/sign-up/email") return;

      const { verifyLocalFirstIdentityProof } = await import(
        /* turbopackIgnore: true */
        /* webpackIgnore: true */
        "jazz-napi"
      );
      const {
        ok,
        error,
        id: provedUserId,
      } = verifyLocalFirstIdentityProof(ctx.body?.proofToken, "next-localfirst-signup");
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
        // Parameter types are inferred from BetterAuth's config types; no
        // explicit annotations are needed. `context` is BetterAuth's
        // GenericEndpointContext; we read `body.provedUserId` which was
        // added by the `hooks.before` middleware above.
        before: async (user, context) => {
          const provedUserId = (context?.body as { provedUserId?: string } | undefined)
            ?.provedUserId;
          if (provedUserId) {
            return { data: { ...user, id: provedUserId } };
          }
        },
      },
    },
  },
  plugins: [
    nextCookies(),
    bearer(),
    jwt({
      jwks: {
        keyPairConfig: { alg: "ES256" },
      },
      jwt: {
        expirationTime: "1h",
        issuer: APP_ORIGIN,
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
  ],
});

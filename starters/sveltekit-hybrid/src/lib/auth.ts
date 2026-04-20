import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { bearer, jwt } from "better-auth/plugins";
import { APIError, createAuthMiddleware } from "better-auth/api";
import { sveltekitCookies } from "better-auth/svelte-kit";
import { getRequestEvent } from "$app/server";
import { env } from "$env/dynamic/private";

const APP_ORIGIN = env.APP_ORIGIN ?? "http://localhost:5173";

if (!env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Run 'pnpm install' to generate .env.local, or set it explicitly in your environment.",
  );
}

const BETTER_AUTH_SECRET = env.BETTER_AUTH_SECRET;

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
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  hooks: {
    // Gate email sign-up on a valid local-first proof token. The signup form
    // mints this by asking Jazz to sign a short-lived token bound to the
    // browser's anonymous Jazz identity; BetterAuth then carries that
    // identity forward so the user's existing local data survives the
    // upgrade.
    before: createAuthMiddleware(async (ctx) => {
      if (ctx.path !== "/sign-up/email") return;

      const { verifyLocalFirstIdentityProof } = await import("jazz-napi");
      const {
        ok,
        error,
        id: provedUserId,
      } = verifyLocalFirstIdentityProof(ctx.body?.proofToken, "sveltekit-localfirst-signup");
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
    // sveltekitCookies must be the last plugin
    sveltekitCookies(getRequestEvent),
  ],
});

import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { APIError, createAuthMiddleware } from "better-auth/api";

const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3000";

if (!process.env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Scaffold via create-jazz, or add it to .env manually (e.g. openssl rand -base64 32).",
  );
}

const BETTER_AUTH_SECRET = process.env.BETTER_AUTH_SECRET;

// TODO: Replace with a persistent adapter before shipping.
//
// Pinning to globalThis is a dev-only workaround: Next.js + Turbopack
// evaluates API route modules and server component modules in separate
// module graphs, so a plain module-local `const` would give each one
// its own `MemoryDB` — sign-up would write to the API route's copy and
// the server component's session check would look in an empty one and
// return null. A real database adapter makes both concerns disappear.
const globalForAuth = globalThis as unknown as { __authMemoryDb?: MemoryDB };
const authMemoryDb: MemoryDB = (globalForAuth.__authMemoryDb ??= {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
});

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

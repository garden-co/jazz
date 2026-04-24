import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { bearer, jwt } from "better-auth/plugins";
import { APIError, createAuthMiddleware } from "better-auth/api";
import { jwtPayload } from "./jwt-payload.js";

const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3001";

if (!process.env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Scaffold via create-jazz, or add it to .env manually (e.g. openssl rand -base64 32).",
  );
}

const BETTER_AUTH_SECRET = process.env.BETTER_AUTH_SECRET;

// TODO: Replace with a persistent adapter before shipping.
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
  trustedOrigins: (request) => {
    const origin = request?.headers.get("origin");
    if (origin && new URL(origin).hostname === "localhost") return [origin];
    return [APP_ORIGIN];
  },
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  hooks: {
    before: createAuthMiddleware(async (ctx) => {
      if (ctx.path !== "/sign-up/email") return;

      const { verifyLocalFirstIdentityProof } = await import("jazz-napi");
      const {
        ok,
        error,
        id: provedUserId,
      } = verifyLocalFirstIdentityProof(ctx.body?.proofToken, "react-localfirst-signup");
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
        definePayload: jwtPayload,
      },
    }),
  ],
});

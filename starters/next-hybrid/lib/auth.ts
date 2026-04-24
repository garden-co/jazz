import { betterAuth } from "better-auth";
import { APIError, createAuthMiddleware } from "better-auth/api";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../schema";
import { authJazzContext } from "./auth-jazz-context";

const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3000";

if (!process.env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Scaffold via create-jazz, or add it to .env manually (e.g. openssl rand -base64 32).",
  );
}

const BETTER_AUTH_SECRET = process.env.BETTER_AUTH_SECRET;

export const auth = betterAuth({
  baseURL: APP_ORIGIN,
  secret: BETTER_AUTH_SECRET,
  database: jazzAdapter({
    db: () => authJazzContext().asBackend(app),
    schema: app.wasmSchema,
  }),
  trustedOrigins: [APP_ORIGIN],
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
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
        // Carry `provedUserId` (stashed by the `hooks.before` middleware) into
        // BetterAuth as the row id, so the freshly-signed-up user inherits the
        // Jazz principal that their local-first data is already scoped to.
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

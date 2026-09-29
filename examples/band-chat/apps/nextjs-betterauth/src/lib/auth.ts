import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";
import { authJazzClient } from "./auth-jazz-client";
import { assertConfiguration, JWT_AUDIENCE } from "./config.mjs";

// Fails closed: no fallback secret, and a deployment must name its origin.
const config = assertConfiguration();
const appOrigin = config.origin;

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins: [appOrigin],
  secret: config.betterAuthSecret,
  database: jazzAdapter({
    db: async () => (await authJazzClient()).db,
    schema: app.wasmSchema,
  }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [
    bearer(),
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: appOrigin,
        audience: JWT_AUDIENCE,
        expirationTime: "1h",
        // Better Auth's stable internal user id remains the raw session user
        // id. Jazz independently records issuer-scoped session.user.
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
    nextCookies(),
  ],
});

import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins: [appOrigin],
  secret: process.env.BETTER_AUTH_SECRET ?? "band-chat-development-secret",
  database: jazzAdapter({
    db: () => authJazzContext().asBackend(app),
    schema: app.wasmSchema,
  }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [
    nextCookies(),
    bearer(),
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: appOrigin,
        expirationTime: "1h",
        // Better Auth's stable internal user id remains the raw session user
        // id. Jazz independently records issuer-scoped session.author.
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
  ],
});

import { betterAuth } from "better-auth";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../schema.js";
import { authJazzContext } from "./auth-jazz-context.js";

const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3001";

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
  ],
});

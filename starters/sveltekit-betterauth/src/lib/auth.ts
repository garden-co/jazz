import { getRequestEvent } from "$app/server";
import { env } from "$env/dynamic/private";
import { betterAuth } from "better-auth";
import { bearer, jwt } from "better-auth/plugins";
import { sveltekitCookies } from "better-auth/svelte-kit";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "./schema";
import { authJazzContext } from "./auth-jazz-context";

const APP_ORIGIN = env.APP_ORIGIN ?? "http://localhost:5173";

if (!env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Scaffold via create-jazz, or add it to .env manually (e.g. openssl rand -base64 32).",
  );
}

const BETTER_AUTH_SECRET = env.BETTER_AUTH_SECRET;

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
    minPasswordLength: 1,
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
    // sveltekitCookies must be the last plugin
    sveltekitCookies(getRequestEvent),
  ],
});

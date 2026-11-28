import { betterAuth } from "better-auth";
import { sveltekitCookies } from "better-auth/svelte-kit";
import { getRequestEvent } from "$app/server";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../../jazz.generated";
import { serverConfig } from "./config";
import { jazz } from "./jazz";

function createAuth() {
  const config = serverConfig();
  return betterAuth({
    baseURL: config.origin,
    // Tests only: dev/examples-e2e/run.mjs sets this for the production server
    // its Playwright suite starts, whose browsers all sign in from one address
    // and share Better Auth's 3-per-10-s sign-in limit. Never set it in a deployment.
    rateLimit:
      process.env.JAZZ_E2E_DISABLE_AUTH_RATE_LIMIT === "1" ? { enabled: false } : undefined,
    trustedOrigins: [config.origin],
    secret: config.betterAuthSecret,
    // Better Auth's rows live in the backend's own Jazz database.
    database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
    emailAndPassword: {
      enabled: true,
      autoSignIn: true,
      minPasswordLength: 8,
      requireEmailVerification: false,
    },
    plugins: [sveltekitCookies(getRequestEvent)],
  });
}

let instance: ReturnType<typeof createAuth> | undefined;

/** Created on first use, so a build never needs the runtime configuration. */
export function auth() {
  return (instance ??= createAuth());
}

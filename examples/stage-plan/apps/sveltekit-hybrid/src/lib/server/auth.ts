import { betterAuth } from "better-auth";
import { sveltekitCookies } from "better-auth/svelte-kit";
import { getRequestEvent } from "$app/server";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../../schema";
import { serverConfig } from "./config";
import { jazz } from "./jazz";

function createAuth() {
  const config = serverConfig();
  return betterAuth({
    baseURL: config.origin,
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

import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";
import { assertConfiguration } from "./config.mjs";
import { jazz } from "./jazz.server";

// Fails closed: no fallback secret, and a deployment must name its origin.
const config = assertConfiguration();
const appOrigin = config.origin;

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins: [appOrigin],
  secret: config.betterAuthSecret,
  // Better Auth's tables live in BandChat's own Jazz, through the backend's
  // database; their client policies deny everything.
  database: jazzAdapter({
    db: async () => (await jazz()).db,
    schema: app.wasmSchema,
  }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [nextCookies()],
});

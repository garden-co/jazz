import { betterAuth } from "better-auth";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../schema";
import { assertConfiguration } from "../src/lib/config.mjs";
import { jazz } from "./jazz";

// Fails closed: no fallback secret, and a deployment must name its origin.
const config = assertConfiguration();

export const auth = betterAuth({
  baseURL: config.origin,
  trustedOrigins: [config.origin],
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
});

import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../jazz.generated";
import { assertConfiguration } from "./config.mjs";
import { jazz } from "./jazz.server";

// Fails closed: no fallback secret, and a deployment must name its origin.
const config = assertConfiguration();
const appOrigin = config.origin;

export const auth = betterAuth({
  baseURL: appOrigin,
  // Tests only: dev/examples-e2e/run.mjs sets this for the production server
  // its Playwright suite starts, whose browsers all sign in from one address
  // and share Better Auth's 3-per-10-s sign-in limit. Never set it in a deployment.
  rateLimit: process.env.JAZZ_E2E_DISABLE_AUTH_RATE_LIMIT === "1" ? { enabled: false } : undefined,
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

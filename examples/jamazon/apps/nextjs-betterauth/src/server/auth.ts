import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/jazz.generated";
import { backend } from "./backend";
import { serverConfig } from "./config";

export const auth = betterAuth({
  baseURL: serverConfig.origin,
  // Tests only: dev/examples-e2e/run.mjs sets this for the production server
  // its Playwright suite starts, whose browsers all sign in from one address
  // and share Better Auth's 3-per-10-s sign-in limit. Never set it in a deployment.
  rateLimit: process.env.JAZZ_E2E_DISABLE_AUTH_RATE_LIMIT === "1" ? { enabled: false } : undefined,
  trustedOrigins: [serverConfig.origin],
  secret: serverConfig.betterAuthSecret,
  database: jazzAdapter({ db: async () => (await backend()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [nextCookies()],
});

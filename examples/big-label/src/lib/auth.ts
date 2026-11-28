import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../jazz.generated";
import { jazz } from "./jazz.server";
import { serverSecret } from "./server-secret";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const auth = betterAuth({
  baseURL: appOrigin,
  // Tests only: dev/examples-e2e/run.mjs sets this for the production server
  // its Playwright suite starts, whose browsers all sign in from one address
  // and share Better Auth's 3-per-10-s sign-in limit. Never set it in a deployment.
  rateLimit: process.env.JAZZ_E2E_DISABLE_AUTH_RATE_LIMIT === "1" ? { enabled: false } : undefined,
  secret: serverSecret(
    "BETTER_AUTH_SECRET",
    "9d1b6e04f3a2c8517d0ea13b8c5f9264d1e6a3b74f0c28d915be46a7f38cd205",
  ),
  trustedOrigins: [appOrigin],
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 8 },
  plugins: [nextCookies()],
});

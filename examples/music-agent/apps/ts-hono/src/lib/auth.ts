import { betterAuth } from "better-auth";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/jazz.generated";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { appOrigin } from "@/src/lib/app-origin";
import { serverSecret } from "@/src/lib/server-secret";

export const auth = betterAuth({
  baseURL: appOrigin,
  // Tests only: dev/examples-e2e/run.mjs sets this for the production server
  // its Playwright suite starts, whose browsers all sign in from one address
  // and share Better Auth's 3-per-10-s sign-in limit. Never set it in a deployment.
  rateLimit: process.env.JAZZ_E2E_DISABLE_AUTH_RATE_LIMIT === "1" ? { enabled: false } : undefined,
  trustedOrigins: [appOrigin],
  secret: serverSecret(
    "BETTER_AUTH_SECRET",
    "music-agent-local-development-secret-not-for-deployments",
  ),
  database: jazzAdapter({ db: async () => (await backendJazzClient()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
});

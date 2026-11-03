import { betterAuth } from "better-auth";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import { appOrigin } from "@/src/lib/app-origin";
import { serverSecret } from "@/src/lib/server-secret";

export const auth = betterAuth({
  baseURL: appOrigin,
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

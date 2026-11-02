import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { jazz } from "@/lib/jazz.server";
import { APP_ORIGIN as appOrigin } from "@/lib/app-origin";
import { serverSecret } from "@/lib/server-secret";

export const auth = betterAuth({
  baseURL: appOrigin,
  secret: serverSecret("BETTER_AUTH_SECRET", "wequencer-development-secret"),
  trustedOrigins: [appOrigin],
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    // Industry-standard minimum; tune to whatever your product requires.
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [nextCookies()],
});

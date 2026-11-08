import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { jazz } from "./jazz.server";
import { appOrigin, serverSecret, trustedOrigins } from "./config";

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins,
  secret: serverSecret("BETTER_AUTH_SECRET"),
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [nextCookies()],
});

import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { backend } from "./backend";
import { serverConfig } from "./config";

export const auth = betterAuth({
  baseURL: serverConfig.origin,
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

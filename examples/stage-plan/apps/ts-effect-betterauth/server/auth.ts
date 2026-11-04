import { betterAuth } from "better-auth";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../schema";
import { serverConfig } from "./config";
import { jazz } from "./jazz";

const config = serverConfig();

export const auth = betterAuth({
  baseURL: config.origin,
  trustedOrigins: [config.origin],
  secret: config.betterAuthSecret,
  // Better Auth's rows live in the backend's own Jazz database.
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
});

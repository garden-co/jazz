import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";
import { readConfig } from "./config.mjs";
import { jazz } from "./jazz.server";

const config = readConfig();

export const auth = betterAuth({
  baseURL: config.origin,
  trustedOrigins: [config.origin],
  secret: config.betterAuthSecret,
  // Better Auth's tables live in RecordPlayer's own Jazz, through the
  // backend's database; their client policies deny everything.
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 8 },
  plugins: [nextCookies()],
});

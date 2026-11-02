import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";
import { jazz } from "./jazz.server";
import { serverSecret } from "./server-secret";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const auth = betterAuth({
  baseURL: appOrigin,
  secret: serverSecret(
    "BETTER_AUTH_SECRET",
    "9d1b6e04f3a2c8517d0ea13b8c5f9264d1e6a3b74f0c28d915be46a7f38cd205",
  ),
  trustedOrigins: [appOrigin],
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 8 },
  plugins: [nextCookies()],
});

import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { jazz } from "@/src/lib/jazz.server";
import { serverSecret } from "@/src/lib/server-secret";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins: [appOrigin],
  secret: serverSecret(
    "BETTER_AUTH_SECRET",
    "2SNhYRceYvKf1HnJ7mQxB3aWd6LeP9tR4uCg8Vz0Ds5FiOoAbXkMwZq",
  ),
  // Better Auth's tables live in PosterShop's own Jazz, through the backend's
  // database; their client policies deny everything.
  database: jazzAdapter({ db: async () => (await jazz()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [nextCookies()],
});

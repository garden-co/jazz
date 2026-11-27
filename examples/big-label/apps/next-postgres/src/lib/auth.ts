import { betterAuth, type BetterAuthOptions } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import pg from "pg";
import { DATABASE_URL } from "./db.server";
import { serverSecret } from "./server-secret";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const authOptions = {
  baseURL: appOrigin,
  secret: serverSecret(
    "BETTER_AUTH_SECRET",
    "9d1b6e04f3a2c8517d0ea13b8c5f9264d1e6a3b74f0c28d915be46a7f38cd205",
  ),
  trustedOrigins: [appOrigin],
  // Better Auth's tables live in Postgres' "auth" schema, next to the app's.
  database: new pg.Pool({ connectionString: DATABASE_URL, options: "-c search_path=auth" }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 8 },
  plugins: [nextCookies()],
} satisfies BetterAuthOptions;

export const auth = betterAuth(authOptions);

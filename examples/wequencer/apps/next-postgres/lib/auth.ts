import { betterAuth, type BetterAuthOptions } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import pg from "pg";
import { APP_ORIGIN as appOrigin } from "@/lib/app-origin";
import { DATABASE_URL } from "@/lib/db.server";
import { serverSecret } from "@/lib/server-secret";

export const authOptions = {
  baseURL: appOrigin,
  secret: serverSecret("BETTER_AUTH_SECRET", "wequencer-development-secret"),
  trustedOrigins: [appOrigin],
  // Better Auth's tables live in Postgres' "auth" schema, next to the app's.
  database: new pg.Pool({ connectionString: DATABASE_URL, options: "-c search_path=auth" }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    // Industry-standard minimum; tune to whatever your product requires.
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [nextCookies()],
} satisfies BetterAuthOptions;

export const auth = betterAuth(authOptions);

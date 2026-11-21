import { betterAuth, type BetterAuthOptions } from "better-auth";
import pg from "pg";
import { DATABASE_URL } from "./db";

const origin = process.env.APP_ORIGIN ?? "http://127.0.0.1:3000";
const secret = process.env.BETTER_AUTH_SECRET;
// Fails closed: no fallback secret.
if (!secret) throw new Error("BandChat needs BETTER_AUTH_SECRET");

export const authOptions = {
  baseURL: origin,
  trustedOrigins: [origin],
  secret,
  // Better Auth's tables live in Postgres' "auth" schema, next to the app's.
  database: new pg.Pool({ connectionString: DATABASE_URL, options: "-c search_path=auth" }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
} satisfies BetterAuthOptions;

export const auth = betterAuth(authOptions);

/** The signed-in user's id (the app's `author`), from the request's session cookie. */
export async function userIdFrom(headers: Headers) {
  const session = await auth.api.getSession({ headers });
  return session?.user.id;
}

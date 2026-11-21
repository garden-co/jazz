import { betterAuth, type BetterAuthOptions } from "better-auth";
import pg from "pg";

export const DATABASE_URL =
  process.env.ZERO_UPSTREAM_DB ?? "postgres://postgres:pass@127.0.0.1:5432/band_chat_zero";
const origin = process.env.APP_ORIGIN ?? "http://127.0.0.1:3000";
const secret = process.env.BETTER_AUTH_SECRET;
// Fails closed: no fallback secret.
if (!secret) throw new Error("BandChat needs BETTER_AUTH_SECRET");

export const authOptions = {
  baseURL: origin,
  trustedOrigins: [origin],
  secret,
  // Better Auth's tables live in Postgres' "auth" schema, which Zero doesn't sync.
  database: new pg.Pool({ connectionString: DATABASE_URL, options: "-c search_path=auth" }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
} satisfies BetterAuthOptions;

export const auth = betterAuth(authOptions);

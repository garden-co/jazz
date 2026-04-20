import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { bearer, jwt } from "better-auth/plugins";
import { sveltekitCookies } from "better-auth/svelte-kit";
import { getRequestEvent } from "$app/server";
import { env } from "$env/dynamic/private";

const APP_ORIGIN = env.APP_ORIGIN ?? "http://localhost:5173";

if (!env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Run 'pnpm install' to generate .env.local, or set it explicitly in your environment.",
  );
}

const BETTER_AUTH_SECRET = env.BETTER_AUTH_SECRET;

// In-memory store — wiped on every dev-server restart. Good enough for a
// starter; swap for a persistent adapter when you wire up a real database.
const authMemoryDb: MemoryDB = {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
};

export const auth = betterAuth({
  baseURL: APP_ORIGIN,
  secret: BETTER_AUTH_SECRET,
  database: memoryAdapter(authMemoryDb),
  trustedOrigins: [APP_ORIGIN],
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 1,
    requireEmailVerification: false,
  },
  plugins: [
    bearer(),
    jwt({
      jwks: {
        keyPairConfig: { alg: "ES256" },
      },
      jwt: {
        expirationTime: "1h",
        issuer: APP_ORIGIN,
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
    // sveltekitCookies must be the last plugin
    sveltekitCookies(getRequestEvent),
  ],
});

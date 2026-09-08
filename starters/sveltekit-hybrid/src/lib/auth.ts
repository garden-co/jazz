import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { bearer, jwt } from "better-auth/plugins";
import { sveltekitCookies } from "better-auth/svelte-kit";
import { getRequestEvent } from "$app/server";
import { env } from "$env/dynamic/private";

const APP_ORIGIN = env.APP_ORIGIN ?? "http://localhost:5173";

if (!env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Scaffold via create-jazz, or add it to .env manually (e.g. openssl rand -base64 32).",
  );
}

const BETTER_AUTH_SECRET = env.BETTER_AUTH_SECRET;

// In-memory store. Each Node process keeps its own copy, so users that sign
// up in one worker cannot sign in from another: HMR reloads, multi-worker
// deploys, and serverless invocations all reset state. Good enough for a
// starter you run locally; swap for a persistent adapter before you ship.
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
    minPasswordLength: 8,
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

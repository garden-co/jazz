import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";

const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://localhost:3000";

if (!process.env.BETTER_AUTH_SECRET) {
  throw new Error(
    "BETTER_AUTH_SECRET is not set. Run 'pnpm install' to generate .env.local, or set it explicitly in your environment.",
  );
}

const BETTER_AUTH_SECRET = process.env.BETTER_AUTH_SECRET;

// TODO: Replace with a persistent adapter before shipping.
//
// Pinning to globalThis is a dev-only workaround: Next.js + Turbopack
// evaluates API route modules and server component modules in separate
// module graphs, so a plain module-local `const` would give each one
// its own `MemoryDB` — sign-up would write to the API route's copy and
// the server component's session check would look in an empty one and
// return null. A real database adapter makes both concerns disappear.
const globalForAuth = globalThis as unknown as { __authMemoryDb?: MemoryDB };
const authMemoryDb: MemoryDB = (globalForAuth.__authMemoryDb ??= {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
});

export const auth = betterAuth({
  baseURL: APP_ORIGIN,
  secret: BETTER_AUTH_SECRET,
  database: memoryAdapter(authMemoryDb),
  trustedOrigins: [APP_ORIGIN],
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    // Industry-standard minimum; tune to whatever your product requires.
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [
    nextCookies(),
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
  ],
});

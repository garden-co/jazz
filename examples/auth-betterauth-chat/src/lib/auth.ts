import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { nextCookies } from "better-auth/next-js";
import { admin, bearer, jwt } from "better-auth/plugins";
import { APP_ORIGIN, DEFAULT_APP_ID } from "../../constants";
import { verifySelfSignedProofToken } from "./verify-self-signed-proof";

const authMemoryDb: MemoryDB = {
  account: [],
  jwks: [],
  session: [],
  user: [],
  verification: [],
};

const BETTER_AUTH_SECRET = "auth-betterauth-chat-development-secret";

async function createBetterAuth(issuer: string = APP_ORIGIN) {
  const auth = betterAuth({
    baseURL: issuer,
    database: memoryAdapter(authMemoryDb),
    secret: BETTER_AUTH_SECRET,
    trustedOrigins: [APP_ORIGIN],
    emailAndPassword: {
      enabled: true,
      autoSignIn: true,
      minPasswordLength: 1,
      requireEmailVerification: false,
    },
    databaseHooks: {
      user: {
        create: {
          async before(user, ctx) {
            // If the client sent a self-signed proof token, verify it and
            // use the proven userId so that the BetterAuth account preserves
            // the same identity the user had before signing up.
            const proofToken = ctx?.headers?.get("x-jazz-self-signed-proof");
            if (proofToken) {
              const provedUserId = verifySelfSignedProofToken(proofToken, DEFAULT_APP_ID);
              return { data: { ...user, id: provedUserId } };
            }
            return { data: user };
          },
        },
      },
    },
    plugins: [
      nextCookies(),
      admin({
        adminRoles: ["admin"],
        defaultRole: "member",
      }),
      bearer(),
      jwt({
        jwks: {
          keyPairConfig: { alg: "ES256" },
        },
        jwt: {
          expirationTime: "10s",
          issuer,
          definePayload: ({ user }: { user: { name: string; role?: string | string[] } }) => ({
            claims: {
              role: Array.isArray(user.role) ? user.role[0] : (user.role ?? ""),
            },
            username: user.name,
          }),
          getSubject: ({ user }: { user: { id: string } }) => user.id,
        },
      }),
    ],
  });

  await auth.api
    .createUser({
      body: {
        email: "admin@example.com",
        name: "admin",
        password: "admin",
        role: "admin",
      },
    })
    .catch(() => {});

  return auth;
}

export const auth = createBetterAuth();

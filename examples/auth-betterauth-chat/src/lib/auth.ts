import { betterAuth } from "better-auth";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { nextCookies } from "better-auth/next-js";
import { admin, bearer, jwt } from "better-auth/plugins";
import { APIError, createAuthMiddleware } from "better-auth/api";
import { verifySelfSignedToken } from "jazz-napi";
import { APP_ORIGIN } from "../../constants";

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
    hooks: {
      before: createAuthMiddleware(async (ctx) => {
        if (ctx.path !== "/sign-up/email") return;

        const proofToken = ctx.body?.proofToken;
        if (!proofToken) {
          throw new APIError("BAD_REQUEST", {
            message: "proofToken is required for sign-up",
          });
        }

        let provedUserId: string;
        try {
          provedUserId = verifySelfSignedToken(proofToken, "betterauth-signup");
        } catch {
          throw new APIError("UNAUTHORIZED", {
            message: "Invalid proof token",
          });
        }

        return {
          context: {
            ...ctx,
            body: { ...ctx.body, provedUserId },
          },
        };
      }),
    },
    databaseHooks: {
      user: {
        create: {
          before: async (user: any, ctx: any) => {
            const provedUserId = ctx?.context?.body?.provedUserId;
            if (provedUserId) {
              return { data: { ...user, id: provedUserId } };
            }
          },
        },
      },
    },
    emailAndPassword: {
      enabled: true,
      autoSignIn: true,
      minPasswordLength: 1,
      requireEmailVerification: false,
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

import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { admin, bearer, jwt } from "better-auth/plugins";
import { APIError, createAuthMiddleware } from "better-auth/api";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { authJazzContext } from "./auth-jazz-context";
import { app } from "../../schema";

const BETTER_AUTH_SECRET = "auth-betterauth-chat-development-secret";
const APP_ORIGIN = process.env.NEXT_PUBLIC_APP_ORIGIN!;

async function createBetterAuth(issuer: string = APP_ORIGIN) {
  const auth = betterAuth({
    baseURL: issuer,
    database: jazzAdapter({
      db: () => authJazzContext().asBackend(app),
      schema: app.wasmSchema,
    }),
    secret: BETTER_AUTH_SECRET,
    trustedOrigins: [APP_ORIGIN],
    hooks: {
      before: createAuthMiddleware(async (ctx) => {
        if (ctx.path !== "/sign-up/email") return;

        const { verifyLocalFirstIdentityProof } = await import(
          /* turbopackIgnore: true */
          /* webpackIgnore: true */
          "jazz-napi"
        );
        const {
          ok,
          error,
          id: provedUserId,
        } = verifyLocalFirstIdentityProof(ctx.body?.proofToken, "betterauth-signup");
        if (!ok) {
          throw new APIError("BAD_REQUEST", { message: error });
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
            const provedUserId = ctx?.body?.provedUserId;
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
          expirationTime: "1h",
          issuer,
          definePayload: ({
            user,
          }: {
            user: { id: string; name: string; role?: string | string[] };
          }) => ({
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

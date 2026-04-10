import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { admin, bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { authJazzContext } from "./auth-jazz-context";
import { app } from "../../schema-better-auth/schema";

const BETTER_AUTH_SECRET = "auth-betterauth-chat-development-secret";
const APP_ORIGIN = process.env.NEXT_PUBLIC_APP_ORIGIN!;

export const auth = betterAuth({
  baseURL: APP_ORIGIN,
  database: jazzAdapter({
    db: () => authJazzContext.asBackend(app),
    schema: app.wasmSchema,
    durabilityTier: "worker",
  }),
  secret: BETTER_AUTH_SECRET,
  trustedOrigins: [APP_ORIGIN],
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
        expirationTime: "30d",
        issuer: APP_ORIGIN,
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

// Seed admin user
auth.api
  .createUser({
    body: {
      email: "admin@example.com",
      name: "admin",
      password: "admin",
      role: "admin",
    },
  })
  .catch(() => {});

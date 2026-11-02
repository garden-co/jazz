import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { admin } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { jazz } from "./jazz.server";
import { app } from "../../schema";

const BETTER_AUTH_SECRET = "auth-betterauth-chat-development-secret";
const APP_ORIGIN = process.env.NEXT_PUBLIC_APP_ORIGIN!;

async function createBetterAuth() {
  const auth = betterAuth({
    baseURL: APP_ORIGIN,
    database: jazzAdapter({
      db: async () => (await jazz()).db,
      schema: app.wasmSchema,
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
      admin({
        adminRoles: ["admin"],
        defaultRole: "member",
      }),
      nextCookies(),
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

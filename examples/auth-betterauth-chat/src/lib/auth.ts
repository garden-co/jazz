import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { admin } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { jazz } from "./jazz.server";
import { app } from "../../jazz.generated";

const BETTER_AUTH_SECRET = "auth-betterauth-chat-development-secret";
const APP_ORIGIN = process.env.NEXT_PUBLIC_APP_ORIGIN!;

async function createBetterAuth() {
  // #region default-stack-better-auth
  const auth = betterAuth({
    baseURL: APP_ORIGIN,
    // Tests only: dev/examples-e2e/run.mjs sets this for the production server
    // its Playwright suite starts, whose browsers all sign in from one address
    // and share Better Auth's 3-per-10-s sign-in limit. Never set it in a deployment.
    rateLimit:
      process.env.JAZZ_E2E_DISABLE_AUTH_RATE_LIMIT === "1" ? { enabled: false } : undefined,
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
  // #endregion default-stack-better-auth

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

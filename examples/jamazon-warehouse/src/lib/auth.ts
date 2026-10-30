import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { backendJazzClient } from "./auth-jazz-client";
import { appOrigin, serverSecret, trustedOrigins } from "./config";

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins,
  secret: serverSecret("BETTER_AUTH_SECRET"),
  database: jazzAdapter({ db: async () => (await backendJazzClient()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [
    bearer(),
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: appOrigin,
        audience: appOrigin,
        expirationTime: "1h",
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
    nextCookies(),
  ],
});

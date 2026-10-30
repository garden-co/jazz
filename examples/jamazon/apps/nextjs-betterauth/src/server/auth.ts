import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { backend } from "./backend";
import { serverConfig } from "./config";

export const auth = betterAuth({
  baseURL: serverConfig.origin,
  trustedOrigins: [serverConfig.origin],
  secret: serverConfig.betterAuthSecret,
  database: jazzAdapter({ db: async () => (await backend()).db, schema: app.wasmSchema }),
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
        issuer: serverConfig.origin,
        audience: serverConfig.origin,
        expirationTime: "1h",
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
    nextCookies(),
  ],
});

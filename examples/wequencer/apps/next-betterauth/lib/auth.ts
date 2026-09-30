import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { authJazzClient } from "@/lib/auth-jazz-client";
import { APP_ORIGIN as appOrigin } from "@/lib/app-origin";
import { serverSecret } from "@/lib/server-secret";

export const auth = betterAuth({
  baseURL: appOrigin,
  secret: serverSecret("BETTER_AUTH_SECRET", "wequencer-development-secret"),
  trustedOrigins: [appOrigin],
  database: jazzAdapter({ db: async () => (await authJazzClient()).db, schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    // Industry-standard minimum; tune to whatever your product requires.
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
        issuer: appOrigin,
        audience: appOrigin,
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
    nextCookies(),
  ],
});

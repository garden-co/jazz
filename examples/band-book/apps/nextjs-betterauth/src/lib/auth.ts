import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { authJazzClient } from "@/src/lib/auth-jazz-client";
import { appOrigin, jwtAudience, jwtIssuer } from "@/src/lib/config";
import { serverSecret } from "@/src/lib/server-secret";

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins: [appOrigin],
  secret: serverSecret("BETTER_AUTH_SECRET", "band-book-local-development-better-auth-secret"),
  database: jazzAdapter({ db: async () => (await authJazzClient()).db, schema: app.wasmSchema }),
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
        issuer: jwtIssuer,
        audience: jwtAudience,
        expirationTime: "1h",
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
    nextCookies(),
  ],
});

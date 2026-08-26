import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "@/schema";
import { authJazzContext } from "@/src/lib/auth-jazz-context";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export const auth = betterAuth({
  baseURL: appOrigin,
  trustedOrigins: [appOrigin],
  secret:
    process.env.BETTER_AUTH_SECRET ?? "2SNhYRceYvKf1HnJ7mQxB3aWd6LeP9tR4uCg8Vz0Ds5FiOoAbXkMwZq",
  database: jazzAdapter({ db: () => authJazzContext().asBackend(app), schema: app.wasmSchema }),
  emailAndPassword: {
    enabled: true,
    autoSignIn: true,
    minPasswordLength: 8,
    requireEmailVerification: false,
  },
  plugins: [
    nextCookies(),
    bearer(),
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: appOrigin,
        expirationTime: "1h",
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
  ],
});

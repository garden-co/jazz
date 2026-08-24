import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { bearer, jwt } from "better-auth/plugins";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";
import { resolveBandChatAuthSecret } from "./auth-secret";
const origin = process.env.APP_ORIGIN ?? "http://127.0.0.1:3000";
export const auth = betterAuth({
  baseURL: origin,
  trustedOrigins: [origin],
  secret: resolveBandChatAuthSecret(process.env),
  database: jazzAdapter({ db: () => authJazzContext().asBackend(app), schema: app.wasmSchema }),
  emailAndPassword: { enabled: true, autoSignIn: true, minPasswordLength: 8 },
  plugins: [
    nextCookies(),
    bearer(),
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: origin,
        expirationTime: "15m",
        getSubject: ({ user }: { user: { id: string } }) => user.id,
      },
    }),
  ],
});

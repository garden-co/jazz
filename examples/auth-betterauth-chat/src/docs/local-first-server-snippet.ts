import { betterAuth } from "better-auth";
import { jwt } from "better-auth/plugins";

// #region local-first-verify-hook
// Better Auth owns its subjects. Jazz core verifies ordinary provider JWTs
// during linking; no sign-up proof hook or provider user-ID replacement is needed.
export const auth = betterAuth({
  plugins: [
    jwt({
      jwks: { keyPairConfig: { alg: "ES256" } },
      jwt: {
        issuer: "https://your-app.example.com",
        definePayload: ({ user }) => ({ role: (user as { role?: string }).role ?? "" }),
      },
    }),
  ],
});
// #endregion local-first-verify-hook

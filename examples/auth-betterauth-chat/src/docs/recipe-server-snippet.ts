import { betterAuth } from "better-auth";
import { jwt } from "better-auth/plugins";

// #region betterauth-server
export const auth = betterAuth({
  // your database, email config, etc.
  plugins: [
    jwt({
      jwks: {
        keyPairConfig: { alg: "ES256" },
      },
      jwt: {
        issuer: "https://your-app.example.com",
        definePayload: ({ user }) => ({
          claims: { role: (user as { role?: string }).role ?? "" },
        }),
      },
    }),
  ],
});
// #endregion betterauth-server

import { withJazz } from "jazz-tools/dev/next";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  },
  {
    // Without BACKEND_SECRET in the environment, the development server
    // generates one and exposes it to the auth backend as BACKEND_SECRET.
    // A JWKS URL alone is not enough: the server rejects external tokens
    // unless the issuer and audience match what Better Auth emits
    // (src/lib/auth.ts), and account login fails as `invalid account credential`.
    server: {
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer: appOrigin,
      jwtAudience: appOrigin,
    },
  },
);

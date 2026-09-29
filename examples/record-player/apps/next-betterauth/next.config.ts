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
    server: { jwksUrl: `${appOrigin}/api/auth/jwks` },
  },
);

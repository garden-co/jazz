import { withJazz } from "jazz-tools/dev/next";

const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
export default withJazz(
  { reactStrictMode: true, serverExternalPackages: ["jazz-napi", "jazz-tools/backend"] },
  {
    server: {
      backendSecret: process.env.BACKEND_SECRET ?? "big-label-dev-backend",
      jwksUrl: `${origin}/api/auth/jwks`,
    },
  },
);

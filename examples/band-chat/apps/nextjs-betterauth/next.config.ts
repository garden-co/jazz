import { withJazz } from "jazz-tools/dev/next";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  },
  {
    server: {
      backendSecret: process.env.BACKEND_SECRET ?? "band-chat-development-backend-secret",
      jwksUrl: `${appOrigin}/api/auth/jwks`,
    },
  },
);

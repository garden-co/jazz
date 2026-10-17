import { withJazz } from "jazz-tools/dev/next";
import { serverSecret } from "./src/lib/server-secret";

const origin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";
export default withJazz(
  { reactStrictMode: true, serverExternalPackages: ["jazz-napi", "jazz-tools/backend"] },
  {
    server: {
      backendSecret: serverSecret("BACKEND_SECRET", "big-label-dev-backend"),
      jwksUrl: `${origin}/api/auth/jwks`,
      // The Jazz server only admits external JWTs from a configured issuer
      // and audience; Better Auth issues both as the app origin (src/lib/auth.ts).
      jwtIssuer: origin,
      jwtAudience: origin,
    },
  },
);

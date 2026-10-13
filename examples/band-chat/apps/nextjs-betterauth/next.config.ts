import { withJazz } from "jazz-tools/dev/next";
import { assertConfiguration, JWT_AUDIENCE } from "./src/lib/config.mjs";

// Fails closed: a deployment without its origin, Jazz app and secrets does not
// start, and there are no checked-in secrets. `pnpm dev` generates local ones.
const config = assertConfiguration();
const appOrigin = config.origin;

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The app is served on 127.0.0.1 (its Better Auth origin); Next only
    // trusts localhost for dev resources such as the HMR socket.
    allowedDevOrigins: ["127.0.0.1"],
  },
  {
    server: {
      backendSecret: config.backendSecret,
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      // The server accepts only BandChat's own tokens: issued by this origin
      // for this audience.
      jwtIssuer: appOrigin,
      jwtAudience: JWT_AUDIENCE,
    },
  },
);

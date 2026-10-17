import { withJazz } from "jazz-tools/dev/next";

const appOrigin = process.env.APP_ORIGIN ?? "http://localhost:3000";

export default withJazz(
  {},
  {
    server: {
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer: appOrigin,
      jwtAudience: appOrigin,
    },
  },
);

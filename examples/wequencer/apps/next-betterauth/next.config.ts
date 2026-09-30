import { withJazz } from "jazz-tools/dev/next";
import { APP_ORIGIN as appOrigin } from "./lib/app-origin";

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

import { withJazz } from "jazz-tools/dev/next";
import {
  APP_ORIGIN,
  DEFAULT_ADMIN_SECRET,
  DEFAULT_APP_ID,
} from "./constants.ts";

export default withJazz(
  {
    reactStrictMode: true,
  },
  {
    adminSecret: DEFAULT_ADMIN_SECRET,
    server: {
      appId: DEFAULT_APP_ID,
      adminSecret: DEFAULT_ADMIN_SECRET,
      jwksUrl: `${APP_ORIGIN}/api/auth/jwks`,
    },
  },
);

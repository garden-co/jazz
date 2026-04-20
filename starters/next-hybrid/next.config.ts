import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {},
  {
    server: {
      jwksUrl: `${process.env.APP_ORIGIN ?? "http://localhost:3000"}/api/auth/jwks`,
    },
  },
);

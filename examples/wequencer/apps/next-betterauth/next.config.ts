import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {},
  {
    server: {
      jwksUrl: `${process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000"}/api/auth/jwks`,
    },
  },
);

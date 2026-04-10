import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {},
  {
    server: {
      allowAnonymous: true,
      allowDemo: true,
      backendSecret: "dev-backend-secret",
    },
  },
);

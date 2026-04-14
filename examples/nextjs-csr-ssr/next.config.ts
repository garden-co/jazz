import { withJazz } from "jazz-tools/dev/next";

export default withJazz(
  {},
  {
    server: {
      backendSecret: "dev-backend-secret",
    },
  },
);

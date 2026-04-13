import { withJazz } from "jazz-tools/dev/next";

const ADMIN_SECRET = process.env.ADMIN_SECRET!;
const APP_ID = process.env.NEXT_PUBLIC_APP_ID!;
const SYNC_SERVER_URL = process.env.NEXT_PUBLIC_SYNC_SERVER_URL!;

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  },
  {
    adminSecret: ADMIN_SECRET,
    server: {
      appId: APP_ID,
      adminSecret: ADMIN_SECRET,
      jwksUrl: `${SYNC_SERVER_URL}/api/auth/jwks`,
    },
  },
);

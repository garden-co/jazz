import { createAuthClient } from "better-auth/client";
import { jwtClient } from "better-auth/client/plugins";

export const authClient = createAuthClient({
  plugins: [jwtClient()],
});

export type AuthSession = ReturnType<(typeof authClient.useSession)["get"]>;

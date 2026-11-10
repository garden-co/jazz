import { createAuthClient } from "better-auth/client";

/** Better Auth's browser client, on this app's own origin (`/api/auth`). */
export const authClient = createAuthClient();

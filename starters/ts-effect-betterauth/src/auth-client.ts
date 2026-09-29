import { createAuthClient } from "better-auth/client";

export const authClient = createAuthClient();

export type AuthSession = ReturnType<(typeof authClient.useSession)["get"]>;

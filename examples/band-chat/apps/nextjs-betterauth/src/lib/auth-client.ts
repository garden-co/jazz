import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient({ plugins: [jwtClient()] });

export async function getJwtFromBetterAuth(): Promise<string | null> {
  const token = await authClient.token();
  return token.error ? null : (token.data?.token ?? null);
}

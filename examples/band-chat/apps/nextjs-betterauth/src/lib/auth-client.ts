import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient({ plugins: [jwtClient()] });

export async function getJwtFromBetterAuth(): Promise<string | null> {
  try {
    const { data, error } = await authClient.token();
    return error ? null : (data?.token ?? null);
  } catch {
    return null;
  }
}

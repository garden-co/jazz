import { createAuthClient } from "better-auth/svelte";
import { jwtClient } from "better-auth/client/plugins";

export const authClient = createAuthClient({
  plugins: [jwtClient()],
});

export async function getToken(): Promise<string | null> {
  const result = await authClient.token();
  if (result.error) return null;
  return result.data.token;
}

import { createAuthClient } from "better-auth/svelte";

export const authClient = createAuthClient();

export async function getToken(): Promise<string | null> {
  const result = await authClient.$fetch<{ token: string }>("/token", {
    method: "GET",
  });
  if (result.error) return null;
  return result.data.token;
}

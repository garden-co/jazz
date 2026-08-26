import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();

export async function getJwtFromBetterAuth(): Promise<string | null> {
  const { data, error } = await authClient.$fetch<{ token: string }>("/token", { method: "GET" });
  return error ? null : (data?.token ?? null);
}

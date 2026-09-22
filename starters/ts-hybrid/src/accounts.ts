import { authClient } from "./auth-client.js";

export async function getToken(): Promise<string> {
  const { data, error } = await authClient.$fetch<{ token: string }>("/token", { method: "GET" });
  if (error || !data?.token) throw new Error(error?.message ?? "No Better Auth token");
  return data.token;
}

import { createAccountManager } from "jazz-tools";
import { authClient } from "./auth-client.js";

const appId = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
let prepared: Promise<Awaited<ReturnType<typeof createAccountManager>>> | undefined;

export function accounts() {
  if (!appId || !serverUrl)
    throw new Error("VITE_JAZZ_APP_ID and VITE_JAZZ_SERVER_URL must be set");
  return (prepared ??= createAccountManager({ appId, serverUrl }));
}

export async function getToken(): Promise<string> {
  const { data, error } = await authClient.$fetch<{ token: string }>("/token", { method: "GET" });
  if (error || !data?.token) throw new Error(error?.message ?? "No Better Auth token");
  return data.token;
}

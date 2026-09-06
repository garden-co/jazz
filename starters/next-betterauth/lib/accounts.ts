"use client";
import { createAccountManager } from "jazz-tools";
import { authClient } from "./auth-client";
let prepared: Promise<Awaited<ReturnType<typeof createAccountManager>>> | undefined;
export function accounts() {
  const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
  const serverUrl = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
  if (!appId || !serverUrl)
    throw new Error("NEXT_PUBLIC_JAZZ_APP_ID and NEXT_PUBLIC_JAZZ_SERVER_URL must be set");
  return (prepared ??= createAccountManager({ appId, serverUrl }));
}
export async function getToken(): Promise<string> {
  const { data, error } = await authClient.$fetch<{ token: string }>("/token", { method: "GET" });
  if (error || !data?.token) throw new Error(error?.message ?? "No Better Auth token");
  return data.token;
}

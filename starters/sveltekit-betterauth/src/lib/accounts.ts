import { createAccountManager } from "jazz-tools";
import { env } from "$env/dynamic/public";
import { getToken } from "$lib/auth-client";
let prepared: Promise<Awaited<ReturnType<typeof createAccountManager>>> | undefined;
export function accounts() {
  const appId = env.PUBLIC_JAZZ_APP_ID;
  const serverUrl = env.PUBLIC_JAZZ_SERVER_URL;
  if (!appId || !serverUrl)
    throw new Error("PUBLIC_JAZZ_APP_ID and PUBLIC_JAZZ_SERVER_URL must be set");
  return (prepared ??= createAccountManager({ appId, serverUrl }));
}
export async function credential(): Promise<string> {
  const token = await getToken();
  if (!token) throw new Error("No Better Auth token");
  return token;
}

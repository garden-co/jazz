import { getToken } from "$lib/auth-client";

export async function credential(): Promise<string> {
  const token = await getToken();
  if (!token) throw new Error("No Better Auth token");
  return token;
}

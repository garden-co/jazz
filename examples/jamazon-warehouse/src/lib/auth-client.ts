import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();

/** The Jazz bearer token for the signed-in operator, for trusted server routes. */
export async function operatorToken(): Promise<string> {
  const response = await fetch("/api/auth/token", { credentials: "same-origin" });
  const body = response.ok ? ((await response.json()) as { token?: unknown }) : {};
  if (typeof body.token !== "string") throw new Error("Sign in again to continue.");
  return body.token;
}

import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();

/** The Better Auth JWT the Jazz session and the store API routes accept. */
export async function getJwtFromBetterAuth(): Promise<string | null> {
  const response = await fetch("/api/auth/token", { credentials: "same-origin" });
  if (!response.ok) return null;
  const body = (await response.json()) as { token?: unknown };
  return typeof body.token === "string" ? body.token : null;
}

export async function requireBetterAuthToken(): Promise<string> {
  const token = await getJwtFromBetterAuth();
  if (!token) throw new Error("Sign in to continue.");
  return token;
}

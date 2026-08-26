import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();

export async function getJwtFromBetterAuth(): Promise<string | null> {
  try {
    const response = await fetch("/api/auth/token", { credentials: "same-origin" });
    if (!response.ok) return null;
    const body = (await response.json()) as { token?: unknown };
    return typeof body.token === "string" ? body.token : null;
  } catch {
    return null;
  }
}

"use client";

import { jwtClient } from "better-auth/client/plugins";
import { createAuthClient } from "better-auth/react";

// Better Auth's JWT plugin currently crosses two independently-versioned
// client type surfaces in this workspace. Keep that boundary local to this
// copyable example; runtime capability is covered by the provider receipt.
export const authClient: any = createAuthClient({ plugins: [jwtClient() as never] });

export async function getJwtFromBetterAuth(): Promise<string | null> {
  const result = await authClient.token();
  if (result.error) return null;
  return result.data?.token ?? null;
}

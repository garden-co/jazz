import { getJwtFromBetterAuth } from "./auth-client";

/** Server routes read the caller from a Better Auth JWT sent as a bearer. */
async function postWithBearer(path: string, body?: unknown): Promise<Response> {
  const jwt = await getJwtFromBetterAuth();
  if (!jwt) throw new Error("Your session has no Jazz token. Sign in again.");
  return await fetch(path, {
    method: "POST",
    credentials: "same-origin",
    headers: {
      authorization: `Bearer ${jwt}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

/** Ask the server to create the demo workspace. Idempotent, so safe to retry. */
export function bootstrapWorkspace(): Promise<Response> {
  return postWithBearer("/api/bootstrap");
}

/** Ask the server to redeem an invite link for the signed-in account. */
export function redeemInviteLink(token: string): Promise<Response> {
  return postWithBearer("/api/invites/redeem", { token });
}

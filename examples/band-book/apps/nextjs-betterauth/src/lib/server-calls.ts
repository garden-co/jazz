// Server routes read the caller from the Better Auth session cookie, so these
// calls carry no token.
async function post(path: string, body?: unknown): Promise<Response> {
  return await fetch(path, {
    method: "POST",
    credentials: "same-origin",
    headers: body === undefined ? {} : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

/** Ask the server to create the demo workspace. Idempotent, so safe to retry. */
export function bootstrapWorkspace(): Promise<Response> {
  return post("/api/bootstrap");
}

/** Ask the server to redeem an invite link for the signed-in account. */
export function redeemInviteLink(token: string): Promise<Response> {
  return post("/api/invites/redeem", { token });
}

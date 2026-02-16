import type { Request } from "express";
import { JazzClient, type Session } from "jazz-ts";

type RequesterIdentity = {
  userId: string;
  claims: Record<string, unknown>;
};

function verifyJwtAndExtractIdentity(_jwt: string): RequesterIdentity {
  // Replace with your auth provider's JWT verification logic.
  return { userId: "replace-with-verified-sub", claims: {} };
}

// #region backend-request-session-ts
export function requesterSessionFromRequest(req: Request): Session {
  const auth = req.header("authorization");
  if (!auth?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const jwt = auth.slice("Bearer ".length);
  const identity = verifyJwtAndExtractIdentity(jwt);
  return { user_id: identity.userId, claims: identity.claims };
}
// #endregion backend-request-session-ts

// #region backend-request-scoped-client-ts
export function scopedClientForRequest(client: JazzClient, req: Request) {
  const session = requesterSessionFromRequest(req);
  return client.forSession(session);
}
// #endregion backend-request-scoped-client-ts

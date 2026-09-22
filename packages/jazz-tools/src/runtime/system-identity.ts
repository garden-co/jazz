import type { Session } from "./context.js";
import { markTrustedReservedSession, SYSTEM_SESSION_ISSUER } from "./client-session.js";

export const SYSTEM_AUTHOR_ID = "system";

/** @internal Module-owned capability for trusted backend reads and writes. */
export const SYSTEM_READ_SESSION: Session = markTrustedReservedSession({
  issuer: SYSTEM_SESSION_ISSUER,
  user_id: SYSTEM_AUTHOR_ID,
  claims: {},
  authMode: "external",
});

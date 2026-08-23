import type { Session } from "./context.js";
import { SYSTEM_SESSION_ISSUER } from "./client-session.js";

export const SYSTEM_AUTHOR_ID = "system";

export const SYSTEM_READ_SESSION: Session = {
  issuer: SYSTEM_SESSION_ISSUER,
  user_id: SYSTEM_AUTHOR_ID,
  claims: {},
  authMode: "external",
};

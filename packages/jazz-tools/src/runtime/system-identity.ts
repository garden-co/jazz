import type { Session } from "./context.js";

export const SYSTEM_AUTHOR_ID = "93c209ee-dbae-5071-a90d-02f8c0bbcf6a";

export const SYSTEM_READ_SESSION: Session = {
  user_id: SYSTEM_AUTHOR_ID,
  claims: {},
  authMode: "external",
};

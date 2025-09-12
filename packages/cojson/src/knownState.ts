import type { SessionID } from "./exports";
import type { CoValueKnownState } from "./sync";

export function combineKnownStateSessions(
  a: CoValueKnownState["sessions"],
  b: CoValueKnownState["sessions"],
): CoValueKnownState["sessions"] {
  const sessionStates: CoValueKnownState["sessions"] = {};

  for (const sessionID of Object.keys(a).concat(
    Object.keys(b),
  ) as SessionID[]) {
    sessionStates[sessionID] = Math.max(a[sessionID] || 0, b[sessionID] || 0);
  }

  return sessionStates;
}

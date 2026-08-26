import type { Session } from "./context.js";

const sessions = new WeakMap<object, Session | null>();
const trustedReservedSessions = new WeakMap<object, Session>();

export function setDbInternalSession(db: object, session: Session | null): void {
  sessions.set(db, session);
}

/** Package-private capability for framework transport orchestration. */
export function getDbInternalSession(db: object): Session | null {
  return sessions.get(db) ?? null;
}

export function getTrustedReservedSession(config: object): Session | undefined {
  return trustedReservedSessions.get(config);
}

export function setTrustedReservedSession(
  config: object,
  session: Session | null | undefined,
): void {
  if (session) {
    trustedReservedSessions.set(config, session);
  } else {
    trustedReservedSessions.delete(config);
  }
}

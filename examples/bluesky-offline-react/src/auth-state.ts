export type Session = { did: string; token: string };
export type AuthenticationState =
  | { kind: "checking" }
  | { kind: "signed-out" }
  | { kind: "signed-in"; session: Session };

export function keepMountedSession(current: Session | undefined, refreshed: Session) {
  return current?.did === refreshed.did ? current : refreshed;
}

export async function refreshAuthentication(
  cachedSession: Session | undefined,
  request: () => Promise<Response>,
  clearCachedSession: () => void,
): Promise<AuthenticationState> {
  try {
    const response = await request();
    if (response.status === 401 || response.status === 403) {
      clearCachedSession();
      return { kind: "signed-out" };
    }
    if (!response.ok) {
      return cachedSession
        ? { kind: "signed-in", session: cachedSession }
        : { kind: "signed-out" };
    }
    const value: unknown = await response.json();
    if (typeof value !== "object" || value === null
      || !("did" in value) || typeof value.did !== "string"
      || !("token" in value) || typeof value.token !== "string") {
      return cachedSession
        ? { kind: "signed-in", session: cachedSession }
        : { kind: "signed-out" };
    }
    return { kind: "signed-in", session: { did: value.did, token: value.token } };
  } catch {
    return cachedSession
      ? { kind: "signed-in", session: cachedSession }
      : { kind: "signed-out" };
  }
}

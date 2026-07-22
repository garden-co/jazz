export type JazzCredentials = { did: string; token: string };
export type AuthenticationState =
  | { kind: "checking" }
  | { kind: "signed-out" }
  | { kind: "signed-in"; session: JazzCredentials }
  | { kind: "unavailable"; message: string };

export function keepMountedSession(
  current: JazzCredentials | undefined,
  refreshed: JazzCredentials,
) {
  return current?.did === refreshed.did ? current : refreshed;
}

function cachedOrUnavailable(
  cachedCredentials: JazzCredentials | undefined,
  message: string,
): AuthenticationState {
  return cachedCredentials
    ? { kind: "signed-in", session: cachedCredentials }
    : { kind: "unavailable", message };
}

export async function refreshAuthentication(
  cachedCredentials: JazzCredentials | undefined,
  request: () => Promise<Response>,
  clearCachedSession: () => void,
): Promise<AuthenticationState> {
  let response: Response;
  try {
    response = await request();
  } catch {
    return cachedOrUnavailable(cachedCredentials, "Could not reach the BFF to check your session.");
  }

  if (response.status === 401 || response.status === 403) {
    clearCachedSession();
    return { kind: "signed-out" };
  }
  if (!response.ok) {
    return cachedOrUnavailable(
      cachedCredentials,
      `The BFF could not check your session (${response.status}).`,
    );
  }

  let value: unknown;
  try {
    value = await response.json();
  } catch {
    return cachedOrUnavailable(cachedCredentials, "The BFF returned invalid Jazz credentials.");
  }
  if (
    typeof value !== "object" ||
    value === null ||
    !("did" in value) ||
    typeof value.did !== "string" ||
    !("token" in value) ||
    typeof value.token !== "string"
  ) {
    return cachedOrUnavailable(cachedCredentials, "The BFF returned invalid Jazz credentials.");
  }
  return { kind: "signed-in", session: { did: value.did, token: value.token } };
}

export interface SignedInUser {
  username: string;
  role: string;
}

async function authRequest(endpoint: string, body: unknown): Promise<SignedInUser> {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = (await response.json().catch(() => null)) as
    | (Partial<SignedInUser> & { error?: string })
    | null;
  if (!response.ok || !payload?.username) {
    throw new Error(payload?.error ?? `Authentication failed with status ${response.status}`);
  }
  return { username: payload.username, role: payload.role ?? "" };
}

/** Each of these sets or clears the app's own session cookie. */
export function requestSignIn(email: string, password: string): Promise<SignedInUser> {
  return authRequest("/api/auth/sign-in", { email, password });
}

export function requestSignUp(email: string, password: string): Promise<SignedInUser> {
  return authRequest("/api/auth/sign-up", { email, password });
}

export async function requestSignOut(): Promise<void> {
  const response = await fetch("/api/auth/sign-out", { method: "POST" });
  if (!response.ok) throw new Error(`Sign out failed with status ${response.status}`);
}

export async function fetchSignedInUser(): Promise<SignedInUser | null> {
  const response = await fetch("/api/auth/session");
  return response.ok ? ((await response.json()) as SignedInUser | null) : null;
}

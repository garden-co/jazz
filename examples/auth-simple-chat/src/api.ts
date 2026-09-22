import { StoredAuthSession } from "./auth-storage";

async function authRequest(
  endpoint: string,
  email: string,
  password: string,
): Promise<StoredAuthSession> {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });

  const payload = (await response.json().catch(() => null)) as {
    error?: string;
    token?: string;
    username?: string;
  } | null;

  if (!response.ok || !payload?.token || !payload.username) {
    throw new Error(payload?.error ?? `Authentication failed with status ${response.status}`);
  }

  return { token: payload.token, username: payload.username };
}

export function requestSignIn(email: string, password: string): Promise<StoredAuthSession> {
  return authRequest("/api/auth/sign-in", email, password);
}

export function requestSignUp(email: string, password: string): Promise<StoredAuthSession> {
  return authRequest("/api/auth/sign-up", email, password);
}

import { db } from "./db";

type Result = { error?: { message: string } };

/** Asks the app's server (app/api/auth) for an Instant token, then signs in with it. */
async function withToken(path: string, body: object): Promise<Result> {
  const response = await fetch(`/api/auth/${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await response.json().catch(() => ({}));
  if (!response.ok) return { error: { message: json.message ?? "Authentication failed" } };
  await db.auth.signInWithToken(json.token);
  return {};
}

export const authClient = {
  signIn: { email: (input: { email: string; password: string }) => withToken("sign-in", input) },
  signUp: {
    email: (input: { name: string; email: string; password: string }) =>
      withToken("sign-up", input),
  },
  signOut: () => db.auth.signOut(),
};

/** Headers for this app's API routes: the Instant token names the caller. */
export function authHeaders(user: { refresh_token: string }): Record<string, string> {
  return { authorization: `Bearer ${user.refresh_token}` };
}

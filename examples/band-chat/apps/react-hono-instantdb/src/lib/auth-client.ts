import { db } from "./db";

type Result = { error?: { message: string } };

/** Asks the app's server (server/auth.ts) for an Instant token, then signs in with it. */
async function withToken(path: string, body: object): Promise<Result> {
  const response = await fetch(`/api/auth/${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await response.json();
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

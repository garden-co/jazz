import { db } from "@/lib/db";

type Result = { error?: { message: string } };

/** Asks the app's server (app/api/auth) for an Instant token, then signs in with it. */
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
  /** The signed-in user with the name given at sign-up. */
  useSession() {
    const { user, isLoading } = db.useAuth();
    const { data } = db.useQuery(
      user ? { $users: { $: { where: { id: user.id }, fields: ["name"] } } } : null,
    );
    const name = data?.$users[0]?.name ?? user?.email ?? "";
    return {
      data: user ? { user: { id: user.id, name, refreshToken: user.refresh_token } } : null,
      isPending: isLoading,
    };
  },
};

import type { JazzSession } from "./state.js";
import { connectAuthProvider, type AuthProviderConnection } from "./auth-provider.js";

type BetterAuthState = {
  data: { session: { id: string }; user: { id: string } } | null;
  isPending: boolean;
  refetch?(): Promise<unknown>;
  error?: { message?: string } | null;
};
/** Structural interface: no Better Auth code is included in Jazz's runtime. */
export interface BetterAuthClient {
  $store: {
    atoms: Record<
      string,
      {
        get(): BetterAuthState;
        subscribe(listener: (state: BetterAuthState) => void): () => void;
      }
    >;
  };
  $fetch: (
    path: string,
    options: { method: "GET" },
  ) => Promise<{
    data: unknown;
    error?: { message?: string } | null;
  }>;
  signOut(): Promise<{ error?: { message?: string } | null }>;
}

/** Mount once per JazzSession on the client; dispose on unmount.
 * Signup and sign-in only call Better Auth. Use connection.logout() for safe signout.
 */
export function connectBetterAuth<Client>(
  session: JazzSession<Client>,
  auth: BetterAuthClient,
): Omit<AuthProviderConnection, "update" | "logout"> & { logout(): Promise<void> } {
  const connection = connectAuthProvider(session, {
    async getToken() {
      const result = await auth.$fetch("/token", { method: "GET" });
      const data = result.data as { token?: string } | null;
      if (result.error || !data?.token)
        throw new Error(result.error?.message ?? "No Better Auth token");
      return data.token;
    },
  });
  const atom = auth.$store.atoms.session;
  const update = (state: BetterAuthState) =>
    connection.update({
      key: state.data ? `${state.data.user.id}:${state.data.session.id}` : null,
      isPending: state.isPending,
      error: state.error
        ? new Error(state.error.message ?? "Better Auth session failed")
        : undefined,
    });
  const unsubscribe = atom.subscribe(update);
  update(atom.get());
  return {
    getSnapshot: connection.getSnapshot,
    subscribe: connection.subscribe,
    async retry() {
      if (atom.get().error) {
        await atom.get().refetch?.();
        update(atom.get());
      }
      await connection.retry();
    },
    logout: () =>
      connection.logout(async () => {
        const result = await auth.signOut();
        if (result.error) throw new Error(result.error.message ?? "Better Auth sign out failed");
      }),
    dispose() {
      unsubscribe();
      connection.dispose();
    },
  };
}

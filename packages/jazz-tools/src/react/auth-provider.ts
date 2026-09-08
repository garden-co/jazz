import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import type { JazzSession } from "../session/state.js";
import {
  connectAuthProvider,
  type AuthProviderState,
  type AuthProviderSnapshot,
} from "../session/auth-provider.js";
import { connectBetterAuth, type BetterAuthClient } from "../session/better-auth.js";

type Connection = Omit<ReturnType<typeof connectBetterAuth>, "logout">;
const pending: AuthProviderSnapshot = { ready: false, isPending: true };
const noopSubscribe = () => () => {};
function useConnection<C extends Connection>(factory: () => C, dependencies: readonly unknown[]) {
  const [owned, setOwned] = useState<{ connection: C; dependencies: readonly unknown[] }>();
  const connection =
    owned && owned.dependencies.every((value, index) => value === dependencies[index])
      ? owned.connection
      : undefined;
  useEffect(() => {
    const next = factory();
    setOwned({ connection: next, dependencies });
    return () => next.dispose();
  }, dependencies);
  const snapshot = useSyncExternalStore(
    connection?.subscribe ?? noopSubscribe,
    connection?.getSnapshot ?? (() => pending),
    () => pending,
  );
  return { ...snapshot, retry: () => connection?.retry() ?? Promise.resolve(), connection };
}
/** Owns the provider subscription, while useJazzSessionOwner owns the session. */
export function useBetterAuth<Client>(session: JazzSession<Client>, auth: BetterAuthClient) {
  const { connection, ...state } = useConnection(
    () => connectBetterAuth(session, auth),
    [session, auth],
  );
  return { ...state, logout: () => connection?.logout() ?? Promise.resolve() };
}
/** Bridge providers whose state is exposed through a React hook (for example WorkOS). */
export function useAuthProvider<Client>(
  session: JazzSession<Client>,
  state: AuthProviderState,
  options: { getToken(): Promise<string> },
) {
  const latest = useRef(options);
  latest.current = options;
  const { connection, ...snapshot } = useConnection(
    () => connectAuthProvider(session, { getToken: () => latest.current.getToken() }),
    [session],
  );
  useEffect(() => {
    connection?.update(state);
  }, [connection, state.key, state.isPending, state.error]);
  return {
    ...snapshot,
    ready: snapshot.ready && snapshot.key === state.key && !state.isPending && !state.error,
    logout: (action: () => unknown) => connection?.logout(action) ?? Promise.resolve(),
  };
}

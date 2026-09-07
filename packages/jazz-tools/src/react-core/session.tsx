import {
  createContext,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import type { JazzSession, JazzSessionActions, JazzSessionSnapshot } from "../session/state.js";
import { attachJazzSessionConsumer } from "../session/consumer.js";
import { JazzClientProvider, type CoreJazzClient } from "./provider.js";

export type UseJazzSessionResult<Client = CoreJazzClient> = JazzSessionSnapshot<Client> &
  JazzSessionActions;
const SessionContext = createContext<UseJazzSessionResult<any> | null>(null);

/** The active account lifecycle, available in both the data UI and signed-out fallback. */
export function useJazzSession<Client = CoreJazzClient>(): UseJazzSessionResult<Client> {
  const value = useContext(SessionContext);
  if (!value) throw new Error("useJazzSession must be used within <JazzSessionProvider>");
  return value;
}

export type JazzSessionProviderProps<Client extends CoreJazzClient = CoreJazzClient> = {
  session: JazzSession<Client>;
  fallback?: ReactNode;
  children: ReactNode;
};

/** Supplies an externally owned session. Unmounting releases its view, not its client. */
export function JazzSessionProvider<Client extends CoreJazzClient>({
  session,
  fallback = null,
  children,
}: JazzSessionProviderProps<Client>) {
  const snapshot = useSyncExternalStore(
    session.subscribe,
    session.getSnapshot,
    session.getSnapshot,
  );
  const lease = useRef<ReturnType<typeof attachJazzSessionConsumer> | null>(null);
  useLayoutEffect(() => {
    const attached = attachJazzSessionConsumer(session);
    lease.current = attached;
    return () => {
      lease.current = null;
    };
  }, [session]);
  useEffect(() => {
    const attached = lease.current;
    // Releasing in passive cleanup, then a microtask, also covers full unmount:
    // all descendant passive query cleanups complete before shutdown resumes.
    return () => {
      queueMicrotask(() => attached?.release());
    };
  }, [session]);
  useEffect(() => {
    // React has now committed the fallback AND run the removed subtree's
    // passive cleanups (including useSyncExternalStore query subscriptions).
    lease.current?.acknowledge(snapshot);
  }, [session, snapshot]);
  const value = useMemo(() => ({ ...session, ...snapshot }), [session, snapshot]);
  return (
    <SessionContext.Provider value={value}>
      {snapshot.status === "ready" && snapshot.client ? (
        <JazzClientProvider client={snapshot.client}>{children}</JazzClientProvider>
      ) : (
        fallback
      )}
    </SessionContext.Provider>
  );
}

export type ConfiguredJazzSessionProviderProps<Config> = {
  /** Captured at mount. Use a React key to replace an application's configuration. */
  config: Config;
  fallback?: ReactNode;
  children: ReactNode;
};

/** Host adapters supply a factory; no host initialization occurs during render or SSR. */
export function ConfiguredJazzSessionProvider<Config, Client extends CoreJazzClient>({
  config,
  createJazzSession,
  fallback = null,
  children,
}: ConfiguredJazzSessionProviderProps<Config> & {
  createJazzSession: (config: Config) => Promise<JazzSession<Client>>;
}) {
  const initial = useRef({ config, createJazzSession }).current;
  const owner = useRef<{
    active: boolean;
    promise?: Promise<void>;
    session?: JazzSession<Client>;
    closeTimer?: ReturnType<typeof setTimeout>;
  }>({ active: true }).current;
  const [session, setSession] = useState<JazzSession<Client>>();
  const [error, setError] = useState<Error>();
  const start = useMemo(
    () => () => {
      if (owner.promise) return owner.promise;
      setError(undefined);
      owner.promise = Promise.resolve()
        .then(() => initial.createJazzSession(initial.config))
        .then(
          async (created) => {
            owner.session = created;
            if (owner.active) setSession(created);
            else await created.close();
          },
          (reason: unknown) => {
            const failure = reason instanceof Error ? reason : new Error(String(reason));
            owner.promise = undefined;
            if (owner.active) setError(failure);
            throw failure;
          },
        );
      return owner.promise;
    },
    [initial, owner],
  );
  useEffect(() => {
    owner.active = true;
    clearTimeout(owner.closeTimer);
    void start().catch(() => {});
    return () => {
      // StrictMode's immediate effect replay retains this same owner. A real
      // unmount closes only after the consumer's complete cleanup commit.
      owner.closeTimer = setTimeout(() => {
        owner.active = false;
        void owner.session?.close().catch(() => {});
      }, 0);
    };
  }, [owner, start]);
  const startup = useMemo<UseJazzSessionResult<Client>>(() => {
    const unavailable = async () => {
      throw new Error("Jazz session is not ready; retry initialization first");
    };
    return {
      status: error ? "error" : "transitioning",
      ...(error ? { error } : {}),
      createLocalFirst: unavailable,
      restoreLocalFirst: unavailable,
      registerJWT: unavailable,
      loginJWT: unavailable,
      linkJWT: unavailable,
      logout: unavailable,
      close: unavailable,
      retry: start,
    };
  }, [error, start]);
  if (session)
    return (
      <JazzSessionProvider session={session} fallback={fallback}>
        {children}
      </JazzSessionProvider>
    );
  return <SessionContext.Provider value={startup}>{fallback}</SessionContext.Provider>;
}

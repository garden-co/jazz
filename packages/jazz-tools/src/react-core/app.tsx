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
import {
  createJazzAppOwner,
  type JazzApp,
  type JazzAppSnapshot,
  type JazzAuth,
} from "../session/app.js";
import type { JazzSessionActions, JazzSession } from "../session/state.js";
import { JazzClientProvider, type CoreJazzClient } from "./provider.js";

export type JazzAuthState<Client = CoreJazzClient> = JazzAppSnapshot<Client> & {
  /** Failures are represented in state, so event handlers may safely ignore the promise. */
  sessionActions: JazzSessionActions;
  logout(): Promise<void>;
  retry(): Promise<void>;
};
const AuthContext = createContext<JazzAuthState<any> | null>(null);

/** One lifecycle state, including while sign-in, loading, or recovery UI is visible. */
export function useJazzAuth<Client = CoreJazzClient>(): JazzAuthState<Client> {
  const value = useContext(AuthContext);
  if (!value) throw new Error("useJazzAuth must be used within <JazzProvider>");
  return value;
}

export type JazzAppViewProps<Client = CoreJazzClient> = {
  children: ReactNode;
  signedOut?: ReactNode;
  loading?: ReactNode | ((state: JazzAuthState<Client>) => ReactNode);
  error?: ReactNode | ((state: JazzAuthState<Client>) => ReactNode);
};

/** Framework shell only: the shared owner reconciles authentication and owns resources. */
export function ConfiguredJazzAppProvider<Config, Client extends CoreJazzClient>({
  config,
  auth,
  createJazzSession,
  ...view
}: JazzAppViewProps<Client> & {
  config: Config;
  auth?: JazzAuth;
  createJazzSession: (config: Config) => Promise<JazzSession<Client>>;
}) {
  // This creates an inert store. No host or auth subscription starts during render.
  const [app] = useState(() =>
    createJazzAppOwner({ ...config, auth }, createJazzSession, { start: false }),
  );
  useEffect(() => {
    app.updateAuth(auth);
  }, [app, auth]);
  const disposal = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => {
    clearTimeout(disposal.current);
    void app.start().catch(() => {});
    return () => {
      // A real unmount disposes after descendant consumers release their leases.
      // StrictMode's immediate replay cancels disposal and retains the same owner.
      disposal.current = setTimeout(() => {
        void app.dispose().catch(() => {});
      }, 0);
    };
  }, [app]);
  return <JazzAppView app={app} {...view} />;
}

function JazzAppView<Client extends CoreJazzClient>({
  app,
  children,
  signedOut = null,
  loading = null,
  error = null,
}: JazzAppViewProps<Client> & { app: JazzApp<Client> }) {
  const snapshot = useSyncExternalStore(app.subscribe, app.getSnapshot, app.getSnapshot);
  const consumer = useMemo(
    () => ({ lease: undefined as ReturnType<typeof app.attachConsumer> | undefined, epoch: 0 }),
    [app],
  );
  useLayoutEffect(() => {
    // Suspense hides layout effects while passive query subscriptions remain live.
    consumer.lease ??= app.attachConsumer();
  }, [app, consumer]);
  useEffect(() => {
    ++consumer.epoch;
    return () => {
      const epoch = ++consumer.epoch;
      queueMicrotask(() => {
        if (consumer.epoch !== epoch) return;
        consumer.lease?.release();
        consumer.lease = undefined;
      });
    };
  }, [consumer]);
  useEffect(() => {
    // Keep this consumer mounted in every fallback, acknowledging after query cleanup.
    consumer.lease?.acknowledge(snapshot);
  }, [consumer, snapshot]);
  const actions = useMemo(
    () => ({
      sessionActions: app.sessionActions,
      logout: () => app.logout().catch(() => {}),
      retry: () => app.retry().catch(() => {}),
    }),
    [app],
  );
  const state = useMemo(() => ({ ...snapshot, ...actions }), [snapshot, actions]);
  const render = (view: JazzAppViewProps<Client>["loading"]) =>
    typeof view === "function" ? view(state) : view;
  return (
    <AuthContext.Provider value={state}>
      {snapshot.status === "ready" && snapshot.client ? (
        <JazzClientProvider client={snapshot.client}>{children}</JazzClientProvider>
      ) : snapshot.status === "signed-out" ? (
        signedOut
      ) : snapshot.status === "error" ? (
        render(error)
      ) : (
        render(loading)
      )}
    </AuthContext.Provider>
  );
}

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
import { createJazzAppOwner, type JazzApp, type JazzAppSnapshot } from "../session/app.js";
import type { JazzSessionActions, JazzSession } from "../session/state.js";
import { JazzClientProvider, type CoreJazzClient } from "./provider.js";

export type JazzAuthState<Client = CoreJazzClient> = JazzAppSnapshot<Client> & {
  /** Manual account operations reject on failure. */
  sessionActions: JazzSessionActions;
  /**
   * Your auth changed: call this after your own sign-in, sign-out or account
   * switch. Jazz asks your backend who is signed in now and follows its
   * answer. Failures are represented in state.
   */
  reauthenticate(): Promise<void>;
  /** Failures are represented in state, so event handlers may safely ignore the promise. */
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

/** Framework shell only: the shared owner follows the backend's user and owns resources. */
export function ConfiguredJazzAppProvider<Config, Client extends CoreJazzClient>({
  config,
  createJazzClient,
  ...view
}: JazzAppViewProps<Client> & {
  config: Config;
  createJazzClient: (config: Config) => Promise<JazzSession<Client>>;
}) {
  // This creates an inert store. No host subscription starts during render.
  const [app] = useState(() => createJazzAppOwner(config, createJazzClient, { start: false }));
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
}: JazzAppViewProps<Client> & {
  app: JazzApp<Client>;
}) {
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
      reauthenticate: () => app.reauthenticate().catch(() => {}),
      logout: () => app.logout().catch(() => {}),
      retry: () => app.retry().catch(() => {}),
    }),
    [app],
  );
  const state = useMemo<JazzAuthState<Client>>(
    () => ({ ...snapshot, ...actions }),
    [snapshot, actions],
  );
  const render = (view: JazzAppViewProps<Client>["loading"]) =>
    typeof view === "function" ? view(state) : view;
  return (
    <AuthContext.Provider value={state}>
      {state.status === "ready" && state.client ? (
        <JazzClientProvider client={state.client}>{children}</JazzClientProvider>
      ) : state.status === "signed-out" ? (
        signedOut
      ) : state.status === "error" ? (
        render(error)
      ) : (
        render(loading)
      )}
    </AuthContext.Provider>
  );
}

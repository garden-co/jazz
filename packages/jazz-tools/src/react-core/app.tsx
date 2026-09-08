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
  /** Manual account operations reject on failure and require a provider without managed auth. */
  sessionActions: JazzSessionActions;
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

type AuthAdmission =
  | { kind: "manual" }
  | { kind: "better-auth"; client: Extract<JazzAuth, { kind: "better-auth" }>["client"] }
  | { kind: "jwt"; key: string | null; isPending: boolean; error: Error | undefined };

function authAdmission(auth: JazzAuth | undefined): AuthAdmission {
  if (!auth) return { kind: "manual" };
  if (auth.kind === "better-auth") return { kind: auth.kind, client: auth.client };
  return { kind: auth.kind, key: auth.key, isPending: !!auth.isPending, error: auth.error };
}
function sameAdmission(left: AuthAdmission, right: AuthAdmission): boolean {
  if (left.kind !== right.kind) return false;
  if (left.kind === "manual") return true;
  if (left.kind === "better-auth")
    return right.kind === "better-auth" && left.client === right.client;
  return (
    right.kind === "jwt" &&
    left.key === right.key &&
    left.isPending === right.isPending &&
    left.error === right.error
  );
}

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
  const jwt = auth?.kind === "jwt" ? auth : undefined;
  const betterAuthClient = auth?.kind === "better-auth" ? auth.client : undefined;
  const requestedAdmission = useMemo(
    () => authAdmission(auth),
    [auth?.kind, jwt?.key, jwt?.isPending, jwt?.error, betterAuthClient],
  );
  const [appliedAdmission, setAppliedAdmission] = useState(() => requestedAdmission);
  useEffect(() => {
    app.updateAuth(auth);
    // Only admit the descriptor after the shared owner has observed its state.
    // This update is after commit; speculative renders cannot mutate the owner.
    setAppliedAdmission((previous) =>
      sameAdmission(previous, requestedAdmission) ? previous : requestedAdmission,
    );
  }, [app, auth, requestedAdmission]);
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
  return (
    <JazzAppView
      app={app}
      admissionPending={!sameAdmission(appliedAdmission, requestedAdmission)}
      admissionError={auth?.kind === "jwt" ? auth.error : undefined}
      {...view}
    />
  );
}

function JazzAppView<Client extends CoreJazzClient>({
  app,
  admissionPending,
  admissionError,
  children,
  signedOut = null,
  loading = null,
  error = null,
}: JazzAppViewProps<Client> & {
  app: JazzApp<Client>;
  admissionPending: boolean;
  admissionError?: Error;
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
  }, [consumer, snapshot, admissionPending]);
  const actions = useMemo(
    () => ({
      sessionActions: app.sessionActions,
      logout: () => app.logout().catch(() => {}),
      retry: () => app.retry().catch(() => {}),
    }),
    [app],
  );
  const state = useMemo<JazzAuthState<Client>>(
    () => ({
      // New auth props must hide the old client in this render, before any child
      // layout effect can commit. The raw snapshot remains the consumer receipt.
      ...(admissionPending
        ? {
            status: admissionError ? ("error" as const) : ("transitioning" as const),
            error: admissionError,
          }
        : snapshot),
      ...actions,
    }),
    [snapshot, actions, admissionPending, admissionError],
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

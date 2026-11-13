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
  attachJazzSessionConsumer,
  jazzSessionActions,
  ownJazzSession,
  startJazzSession,
  type JazzSession,
  type JazzSessionSnapshot,
} from "../session/jazz-session.js";
import type { SessionCore } from "../session/state.js";
import { JazzClientProvider, type CoreJazzClient } from "./provider.js";

/** `useJazzSession()`: the session's state, and its operations. */
export type UseJazzSessionResult<Client = CoreJazzClient> = JazzSessionSnapshot<Client> &
  Omit<JazzSession<Client>, "getSnapshot" | "subscribe" | "db">;
const SessionContext = createContext<UseJazzSessionResult<any> | null>(null);

/**
 * Who is signed in and what the session is doing, with its operations
 * (`createDeviceAccount`, `refresh`, `signOut`, ...). Available everywhere
 * under `<JazzProvider>`, including its `signedOut`, `loading`, `error` and
 * `closed` views. Operations reject on failure; the failure also shows in `error`.
 */
export function useJazzSession<Client = CoreJazzClient>(): UseJazzSessionResult<Client> {
  const value = useContext(SessionContext);
  if (!value) throw new Error("useJazzSession must be used within <JazzProvider>");
  return value;
}

/** What `<JazzProvider>` shows for each session status. */
export type JazzSessionViewProps<Client = CoreJazzClient> = {
  children: ReactNode;
  /** Shown while no one is signed in. */
  signedOut?: ReactNode;
  /** Shown while the session starts or switches users. */
  loading?: ReactNode | ((state: UseJazzSessionResult<Client>) => ReactNode);
  /** Shown when there is no usable client (`status: "error"`). */
  error?: ReactNode | ((state: UseJazzSessionResult<Client>) => ReactNode);
  /**
   * Shown once the session is closed (`status: "closed"`, after
   * `session.close()`): it has no client and never starts again. Nothing by
   * default. To start over, give the provider a new session (or a new React
   * key for `config`).
   */
  closed?: ReactNode | ((state: UseJazzSessionResult<Client>) => ReactNode);
};

/** The two ways to give `<JazzProvider>` its session. */
export type JazzSessionSourceProps<Config, Client> =
  | {
      /**
       * Open and own a session with this configuration. It's captured at
       * mount: use a React key to replace it.
       */
      config: Config;
      session?: never;
    }
  | {
      /** Show a session you own (from `createJazzClient`); unmounting doesn't close it. */
      session: JazzSession<Client>;
      config?: never;
    };

/** @internal A host's `<JazzProvider>`: its config type, session core and default views. */
export function JazzSessionProvider<Config, Client extends CoreJazzClient>(
  props: JazzSessionSourceProps<Config, Client> &
    JazzSessionViewProps<Client> & {
      openCore: (config: Config) => Promise<SessionCore<Client>>;
    },
) {
  const { config, session: external, openCore, ...view } = props;
  if (external) return <JazzSessionView session={external} {...view} />;
  return <OwnedJazzSession config={config as Config} openCore={openCore} {...view} />;
}

function OwnedJazzSession<Config, Client extends CoreJazzClient>({
  config,
  openCore,
  ...view
}: JazzSessionViewProps<Client> & {
  config: Config;
  openCore: (config: Config) => Promise<SessionCore<Client>>;
}) {
  // An inert session: nothing starts during render (or SSR).
  const [session] = useState(() => ownJazzSession(config, openCore, { start: false }));
  const closing = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => {
    clearTimeout(closing.current);
    void startJazzSession(session as JazzSession<unknown>).catch(reportJazzStartupFailure);
    return () => {
      // A real unmount closes after descendant consumers release their leases.
      // StrictMode's immediate replay cancels this and keeps the same session.
      closing.current = setTimeout(() => {
        void session.close().catch(() => {});
      }, 0);
    };
  }, [session]);
  return <JazzSessionView session={session} {...view} />;
}

function JazzSessionView<Client extends CoreJazzClient>({
  session,
  children,
  signedOut = null,
  loading = null,
  error = null,
  closed = null,
}: JazzSessionViewProps<Client> & { session: JazzSession<Client> }) {
  const snapshot = useSyncExternalStore(
    session.subscribe,
    session.getSnapshot,
    session.getSnapshot,
  );
  const consumer = useMemo(
    () => ({
      lease: undefined as ReturnType<typeof attachJazzSessionConsumer<Client>> | undefined,
      epoch: 0,
    }),
    [session],
  );
  useLayoutEffect(() => {
    // Suspense hides layout effects while passive query subscriptions remain live.
    consumer.lease ??= attachJazzSessionConsumer(session);
  }, [session, consumer]);
  useEffect(() => {
    ++consumer.epoch;
    return () => {
      const epoch = ++consumer.epoch;
      // StrictMode immediately replays passive setup and cancels this release.
      // A real unmount releases after all descendant passive query cleanups.
      queueMicrotask(() => {
        if (consumer.epoch !== epoch) return;
        consumer.lease?.release();
        consumer.lease = undefined;
      });
    };
  }, [consumer]);
  useEffect(() => {
    // The previous view is committed and its query subscriptions detached.
    consumer.lease?.acknowledge(snapshot);
  }, [consumer, snapshot]);
  const actions = useMemo(() => jazzSessionActions(session), [session]);
  const state = useMemo<UseJazzSessionResult<Client>>(
    () => ({ ...snapshot, ...actions }),
    [snapshot, actions],
  );
  const render = (view: JazzSessionViewProps<Client>["loading"]) =>
    typeof view === "function" ? view(state) : view;
  return (
    <SessionContext.Provider value={state}>
      {state.status === "ready" && state.client ? (
        <JazzClientProvider client={state.client}>{children}</JazzClientProvider>
      ) : state.status === "signed-out" ? (
        signedOut
      ) : state.status === "error" ? (
        render(error)
      ) : state.status === "closed" ? (
        render(closed)
      ) : (
        render(loading)
      )}
    </SessionContext.Provider>
  );
}

/**
 * A provider's own start keeps its failure in state for the app's `error`
 * view, which may render nothing. Log it too, so a blank screen still says why.
 */
export function reportJazzStartupFailure(error: unknown) {
  console.error("Jazz could not start:", error);
}

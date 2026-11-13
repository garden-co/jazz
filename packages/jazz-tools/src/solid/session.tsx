import {
  createContext,
  createEffect,
  createSignal,
  onCleanup,
  onMount,
  Match,
  Show,
  Switch,
  useContext,
  type Accessor,
  type JSX,
} from "solid-js";
import type { JazzClient } from "../web/create-jazz-client.js";
import {
  attachJazzSessionConsumer,
  jazzSessionActions,
  ownJazzSession,
  startJazzSession,
  type JazzSession,
  type JazzSessionSnapshot,
} from "../session/jazz-session.js";
import { openBrowserSessionCore, type JazzClientConfig } from "../session/create-jazz-session.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { createSolidJazzClientStore } from "./solid-jazz-client-store.js";
import { JazzClientProvider } from "./provider.js";
import { assertNoClassicProviderProps } from "../classic-api.js";
import {
  refuseAutoAttachDevTools,
  type NoAutoAttachDevTools,
} from "../runtime/removed-provider-props.js";
import type { SolidJazzClient } from "./create-solid-jazz-client.js";

type ProvidedSession = {
  session: JazzSession<JazzClient>;
  snapshot: Accessor<JazzSessionSnapshot<JazzClient>>;
};
const SessionContext = createContext<ProvidedSession>();

/** `useJazzSession()`: the session's state as an accessor, and its operations. */
export type UseJazzSession = Omit<JazzSession<JazzClient>, "getSnapshot" | "subscribe" | "db"> & {
  readonly snapshot: Accessor<JazzSessionSnapshot<JazzClient>>;
};

/**
 * Who is signed in and what the session is doing (`snapshot()`), with its
 * operations (`createDeviceAccount`, `refresh`, `signOut`, ...). Available
 * under `<JazzProvider>`, including its `signedOut`, `loading` and `error`
 * views. Operations reject on failure; the failure also shows in
 * `snapshot().error`.
 */
export function useJazzSession(session?: JazzSession<JazzClient>): UseJazzSession {
  // Under a provider, its own snapshot: it stays current for as long as the
  // provider lives, even in a view that has since been disposed.
  const provided = session ? undefined : useContext(SessionContext);
  if (provided) return withActions(provided.session, provided.snapshot);
  if (!session) throw new Error("useJazzSession requires <JazzProvider> or a session argument");
  const own = session;
  const [snapshot, setSnapshot] = createSignal(own.getSnapshot());
  onCleanup(own.subscribe(() => setSnapshot(own.getSnapshot())));
  return withActions(own, snapshot);
}

function withActions(
  session: JazzSession<JazzClient>,
  snapshot: Accessor<JazzSessionSnapshot<JazzClient>>,
): UseJazzSession {
  return { ...jazzSessionActions(session), snapshot };
}

export type JazzProviderProps = NoAutoAttachDevTools & {
  children: JSX.Element;
  /** Shown while no one is signed in. */
  signedOut?: JSX.Element;
  /** Shown while the session starts or switches users. */
  loading?: JSX.Element;
  /** Shown when there is no usable client (`status: "error"`). */
  error?: (error: Error, retry: () => Promise<void>) => JSX.Element;
  /**
   * Shown once the session is closed (`status: "closed"`, after
   * `session.close()`): it has no client and never starts again. Nothing by
   * default.
   */
  closed?: JSX.Element;
} & (
    | {
        /** Open and own a session with this configuration (captured at creation). */
        config: JazzClientConfig;
        session?: never;
      }
    | {
        /** Show a session you own (from `createJazzClient`); cleanup doesn't close it. */
        session: JazzSession<JazzClient>;
        config?: never;
      }
  );

export function ActiveClient(props: { client: JazzClient; children: JSX.Element }) {
  const client = props.client;
  const state = createSolidJazzClientStore(() => client);
  const adapted: SolidJazzClient = attachSubscriptionStore(
    {
      db: client.db,
      get session() {
        return state.session;
      },
      get authState() {
        return state.authState;
      },
      shutdown: client.shutdown.bind(client),
      loading: false,
      error: undefined,
      state: "ready",
    },
    getSubscriptionStore(client),
  );
  return <JazzClientProvider client={adapted}>{props.children}</JazzClientProvider>;
}
/**
 * Jazz for a Solid app: owns a session (`config`) or shows yours (`session`),
 * renders `children` with its client once someone is signed in, and
 * `signedOut`, `loading` or `error` otherwise, and `closed` (nothing by
 * default) once the session is closed.
 */
export function JazzProvider(props: JazzProviderProps) {
  refuseAutoAttachDevTools("JazzProvider", props);
  assertNoClassicProviderProps("JazzProvider", props);
  if (!props.config === !props.session)
    throw new Error(
      '<JazzProvider> takes either `config` (its session\'s configuration, e.g. config={{ serverUrl: "/jazz" }}) or a `session` from createJazzClient.',
    );
  if (props.config && "account" in props.config)
    throw new Error(
      "<JazzProvider config> is the session's configuration and takes no `account`: the session signs in itself (createDeviceAccount(), or your backend's user). For a client you opened yourself, use <JazzClientProvider client>.",
    );
  const owned = !props.session;
  const session =
    props.session ?? ownJazzSession({ ...props.config! }, openBrowserSessionCore, { start: false });
  const [snapshot, setSnapshot] = createSignal(session.getSnapshot());
  const lease = attachJazzSessionConsumer(session);
  const stop = session.subscribe(() => setSnapshot(session.getSnapshot()));
  onMount(() => {
    if (owned) void startJazzSession(session as JazzSession<unknown>).catch(() => {});
  });
  onCleanup(() => {
    stop();
    lease.release();
    if (owned) void session.close().catch(console.error);
  });
  createEffect(() => {
    const observed = snapshot();
    // Solid's synchronous keyed Show disposal and cleanup run before this queued acknowledgement.
    queueMicrotask(() => lease.acknowledge(observed));
  });
  const retry = () => session.retry().catch(() => {});
  const fallback = () => (
    <Show
      when={snapshot().status === "error" ? snapshot().error : undefined}
      keyed
      fallback={
        <Switch fallback={props.loading ?? <p role="status">Loading...</p>}>
          <Match when={snapshot().status === "signed-out"}>{props.signedOut}</Match>
          <Match when={snapshot().status === "closed"}>{props.closed}</Match>
        </Switch>
      }
    >
      {(error) =>
        props.error ? (
          props.error(error, retry)
        ) : (
          <>
            <p role="alert">{error.message}</p>
            <button onClick={() => void retry()}>Retry</button>
          </>
        )
      }
    </Show>
  );
  return (
    <SessionContext.Provider value={{ session, snapshot }}>
      <Show
        when={snapshot().status === "ready" ? snapshot().client : undefined}
        keyed
        fallback={fallback()}
      >
        {(client) => <ActiveClient client={client}>{props.children}</ActiveClient>}
      </Show>
    </SessionContext.Provider>
  );
}

import {
  createContext,
  createEffect,
  createSignal,
  onCleanup,
  Show,
  useContext,
  type Accessor,
  type JSX,
} from "solid-js";
import type { JazzClient } from "../web/create-jazz-client.js";
import type { JazzSession, JazzSessionActions, JazzSessionSnapshot } from "../session/state.js";
import { attachJazzSessionConsumer } from "../session/consumer.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import { createSolidJazzClientStore } from "./solid-jazz-client-store.js";
import { JazzClientProvider } from "./provider.js";
import {
  refuseAutoAttachDevTools,
  type NoAutoAttachDevTools,
} from "../runtime/removed-provider-props.js";
import type { SolidJazzClient } from "./create-solid-jazz-client.js";

const SessionContext = createContext<JazzSession<JazzClient>>();
export type UseJazzSession = JazzSessionActions & {
  readonly snapshot: Accessor<JazzSessionSnapshot<JazzClient>>;
};
export function useJazzSession(session = useContext(SessionContext)): UseJazzSession {
  if (!session)
    throw new Error("useJazzSession requires JazzSessionProvider or a session argument");
  const [snapshot, setSnapshot] = createSignal(session.getSnapshot());
  onCleanup(session.subscribe(() => setSnapshot(session.getSnapshot())));
  return { ...session, snapshot };
}
export const createJazzSessionState = useJazzSession;
export interface JazzSessionProviderProps extends NoAutoAttachDevTools {
  session: JazzSession<JazzClient>;
  children: JSX.Element;
  fallback?: JSX.Element;
}
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
/** Caller owns the session; this provider owns only its reactive consumer lease. */
export function JazzSessionProvider(props: JazzSessionProviderProps) {
  refuseAutoAttachDevTools("JazzSessionProvider", props);
  const session = props.session;
  const [snapshot, setSnapshot] = createSignal(session.getSnapshot());
  const lease = attachJazzSessionConsumer(session);
  const stop = session.subscribe(() => setSnapshot(session.getSnapshot()));
  onCleanup(() => {
    stop();
    lease.release();
  });
  createEffect(() => {
    const observed = snapshot();
    // Solid's synchronous keyed Show disposal and cleanup run before this queued acknowledgement.
    queueMicrotask(() => lease.acknowledge(observed));
  });
  return (
    <SessionContext.Provider value={session}>
      <Show when={snapshot().client} keyed fallback={props.fallback ?? null}>
        {(client) => <ActiveClient client={client}>{props.children}</ActiveClient>}
      </Show>
    </SessionContext.Provider>
  );
}

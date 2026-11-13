import {
  defineComponent,
  h,
  inject,
  nextTick,
  onBeforeUpdate,
  onMounted,
  onScopeDispose,
  onUnmounted,
  provide,
  shallowReadonly,
  shallowRef,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
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
import { JazzClientProvider } from "./provider.js";
import { assertNoClassicProviderProps } from "../classic-api.js";
import {
  refuseAutoAttachDevTools,
  type NoAutoAttachDevTools,
} from "../runtime/removed-provider-props.js";

type ProvidedSession = {
  session: JazzSession<JazzClient>;
  snapshot: Readonly<ShallowRef<JazzSessionSnapshot<JazzClient>>>;
};
const SessionKey: InjectionKey<ProvidedSession> = Symbol("JazzSession");

/** `useJazzSession()`: the session's state as a ref, and its operations. */
export type UseJazzSession = Omit<JazzSession<JazzClient>, "getSnapshot" | "subscribe" | "db"> & {
  readonly snapshot: Readonly<ShallowRef<JazzSessionSnapshot<JazzClient>>>;
};

/**
 * Who is signed in and what the session is doing (`snapshot`), with its
 * operations (`createDeviceAccount`, `refresh`, `signOut`, ...), which stay
 * bound when destructured. Available under `<JazzProvider>`, in every slot.
 * Operations reject on failure; the failure also shows in `snapshot.error`.
 */
export function useJazzSession(session?: JazzSession<JazzClient>): UseJazzSession {
  // Under a provider, its own snapshot: it stays current for as long as the
  // provider is mounted, even in a view that has since unmounted.
  const provided = session ? undefined : inject(SessionKey, undefined);
  if (provided) return withActions(provided.session, provided.snapshot);
  if (!session) throw new Error("useJazzSession requires <JazzProvider> or a session argument");
  const own = session;
  const snapshot = shallowRef(own.getSnapshot());
  onScopeDispose(
    own.subscribe(() => {
      snapshot.value = own.getSnapshot();
    }),
  );
  return withActions(own, shallowReadonly(snapshot));
}

function withActions(
  session: JazzSession<JazzClient>,
  snapshot: Readonly<ShallowRef<JazzSessionSnapshot<JazzClient>>>,
): UseJazzSession {
  return { ...jazzSessionActions(session), snapshot };
}

export interface JazzProviderProps extends NoAutoAttachDevTools {
  /** Open and own a session with this configuration (captured at setup; use a key to replace it). */
  config?: JazzClientConfig;
  /** Show a session you own (from `createJazzClient`); unmounting doesn't close it. */
  session?: JazzSession<JazzClient>;
}

/**
 * Jazz for a Vue app: owns a session (`config`) or shows yours (`session`),
 * renders the default slot with its client once someone is signed in, and
 * the `signedOut`, `loading` or `error` slot otherwise. Once the session is
 * closed (`session.close()`), it renders the `closed` slot: nothing by default.
 */
export const JazzProvider = defineComponent({
  name: "JazzProvider",
  inheritAttrs: false,
  props: {
    config: Object as PropType<JazzClientConfig>,
    session: Object as PropType<JazzSession<JazzClient>>,
  },
  setup(props, { slots, attrs }) {
    refuseAutoAttachDevTools("JazzProvider", attrs);
    assertNoClassicProviderProps("JazzProvider", attrs);
    // Attributes can arrive later too: refuse Classic configuration then.
    onBeforeUpdate(() => {
      refuseAutoAttachDevTools("JazzProvider", attrs);
      assertNoClassicProviderProps("JazzProvider", attrs);
    });
    if (!props.config === !props.session)
      throw new Error(
        "<JazzProvider> takes either `config` (its session's configuration, e.g. :config=\"{ serverUrl: '/jazz' }\") or a `session` from createJazzClient.",
      );
    if (props.config && "account" in props.config)
      throw new Error(
        "<JazzProvider :config> is the session's configuration and takes no `account`: the session signs in itself (createDeviceAccount(), or your backend's user). For a client you opened yourself, use <JazzClientProvider :client>.",
      );
    const owned = !props.session;
    const session =
      props.session ?? ownJazzSession(props.config!, openBrowserSessionCore, { start: false });
    const snapshot = shallowRef(session.getSnapshot());
    provide(SessionKey, { session, snapshot: shallowReadonly(snapshot) });
    const lease = attachJazzSessionConsumer(session);
    const stop = session.subscribe(() => {
      const observed = session.getSnapshot();
      snapshot.value = observed;
      // Vue flushes old descendants' unmount hooks before acknowledging.
      void nextTick(() => lease.acknowledge(observed));
    });
    onMounted(() => {
      if (owned) void startJazzSession(session as JazzSession<unknown>).catch(() => {});
    });
    onUnmounted(() => {
      stop();
      lease.release();
      if (owned) void session.close().catch(() => {});
    });
    const retry = () => session.retry().catch(() => {});
    const loading = () => slots.loading?.() ?? h("p", { role: "status" }, "Loading Jazz…");
    return () => {
      const state = snapshot.value;
      if (state.status === "ready" && state.client)
        return h(
          JazzClientProvider,
          { client: state.client },
          { default: slots.default, fallback: loading },
        );
      if (state.status === "signed-out") return slots.signedOut?.() ?? null;
      // A closed session has no client and never starts again.
      if (state.status === "closed") return slots.closed?.() ?? null;
      if (state.status === "error")
        return (
          slots.error?.({ error: state.error, retry }) ??
          h("div", { role: "alert" }, [
            h("p", state.error?.message ?? "Jazz could not start"),
            h("button", { type: "button", onClick: () => void retry() }, "Retry"),
          ])
        );
      return loading();
    };
  },
});

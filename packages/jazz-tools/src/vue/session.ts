import {
  defineComponent,
  h,
  inject,
  nextTick,
  onScopeDispose,
  provide,
  shallowReadonly,
  shallowRef,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
import type { JazzClient } from "../web/create-jazz-client.js";
import type { JazzSession, JazzSessionActions, JazzSessionSnapshot } from "../session/state.js";
import { attachJazzSessionConsumer } from "../session/consumer.js";
import { JazzClientProvider } from "./provider.js";

const SessionKey: InjectionKey<JazzSession<JazzClient>> = Symbol("JazzSession");
export type UseJazzSession = JazzSessionActions & {
  readonly snapshot: Readonly<ShallowRef<JazzSessionSnapshot<JazzClient>>>;
};
/** Observe the configured session; commands remain bound when destructured. */
export function useJazzSession(session = inject(SessionKey)): UseJazzSession {
  if (!session)
    throw new Error("useJazzSession requires JazzSessionProvider or a session argument");
  const snapshot = shallowRef(session.getSnapshot());
  onScopeDispose(
    session.subscribe(() => {
      snapshot.value = session.getSnapshot();
    }),
  );
  return { ...session, snapshot: shallowReadonly(snapshot) };
}
export interface JazzSessionProviderProps {
  session: JazzSession<JazzClient>;
  autoAttachDevTools?: boolean;
}
/** Provides a caller-owned session to both active children and the fallback slot. */
export const JazzSessionProvider = defineComponent({
  name: "JazzSessionProvider",
  props: {
    session: { type: Object as PropType<JazzSession<JazzClient>>, required: true },
    autoAttachDevTools: { type: Boolean, default: true },
  },
  setup(props, { slots }) {
    const session = props.session;
    provide(SessionKey, session);
    const snapshot = shallowRef(session.getSnapshot());
    const lease = attachJazzSessionConsumer(session);
    const stop = session.subscribe(() => {
      const observed = session.getSnapshot();
      snapshot.value = observed;
      // Vue's flush includes old descendants' unmount hooks before acknowledgement.
      void nextTick(() => lease.acknowledge(observed));
    });
    onScopeDispose(() => {
      stop();
      lease.release();
    });
    return () =>
      snapshot.value.client
        ? h(
            JazzClientProvider,
            { client: snapshot.value.client, autoAttachDevTools: props.autoAttachDevTools },
            { default: slots.default, fallback: slots.fallback },
          )
        : (slots.fallback?.() ?? null);
  },
});

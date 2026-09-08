import {
  defineComponent,
  h,
  inject,
  nextTick,
  onMounted,
  onUnmounted,
  provide,
  shallowReadonly,
  shallowRef,
  watch,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
import { createJazzAppOwner, type JazzAppSnapshot, type JazzAuth } from "../session/app.js";
import { createJazzSession, type JazzSessionConfig } from "../session/create-jazz-session.js";
import type { AccountDbConfig } from "../accounts/context.js";
import type { JazzClient } from "./create-jazz-client.js";
import { JazzClientProvider, LegacyJazzProvider } from "./provider.js";

export type JazzProviderProps = Partial<JazzSessionConfig> & {
  config?: AccountDbConfig;
  auth?: JazzAuth;
  autoAttachDevTools?: boolean;
};
export type UseJazzAuth = {
  readonly snapshot: Readonly<ShallowRef<JazzAppSnapshot<JazzClient>>>;
  logout(): Promise<void>;
  retry(): Promise<void>;
};
const AuthKey: InjectionKey<UseJazzAuth> = Symbol("JazzAuth");
/** Observe the unified lifecycle from any provider slot. */
export function useJazzAuth(): UseJazzAuth {
  const auth = inject(AuthKey);
  if (!auth) throw new Error("useJazzAuth requires JazzProvider");
  return auth;
}
/** Runtime configuration is captured at setup; use a Vue key to remount it. */
export const JazzProvider = defineComponent({
  name: "JazzProvider",
  inheritAttrs: false,
  props: {
    config: Object as PropType<AccountDbConfig>,
    appId: String,
    serverUrl: String,
    auth: Object as PropType<JazzAuth>,
    initial: [String, Object] as PropType<JazzSessionConfig["initial"]>,
    autoAttachDevTools: { type: Boolean, default: true },
  },
  setup(props, { slots, attrs }) {
    if (props.config)
      return () =>
        h(
          LegacyJazzProvider,
          {
            config: props.config!,
            autoAttachDevTools: props.autoAttachDevTools,
          },
          slots,
        );
    if (!props.appId) throw new Error("JazzProvider requires appId or config");
    const config = {
      ...attrs,
      appId: props.appId,
      serverUrl: props.serverUrl,
      initial: props.initial ?? (props.auth ? undefined : "local-first"),
      auth: props.auth,
    } as JazzSessionConfig & { auth?: JazzAuth };
    const app = createJazzAppOwner(config, createJazzSession, { start: false });
    const snapshot = shallowRef(app.getSnapshot());
    const lease = app.attachConsumer();
    const auth: UseJazzAuth = {
      snapshot: shallowReadonly(snapshot),
      logout: () => app.logout(),
      retry: () => app.retry(),
    };
    provide(AuthKey, auth);
    const stop = app.subscribe(() => {
      const observed = app.getSnapshot();
      snapshot.value = observed;
      // Vue flushes old descendants' unmount hooks before acknowledging.
      void nextTick(() => lease.acknowledge(observed));
    });
    watch(
      () => props.auth,
      (value) => app.updateAuth(value),
    );
    onMounted(() => {
      void app.start().catch(() => {});
    });
    onUnmounted(() => {
      stop();
      lease.release();
      void app.dispose().catch(() => {});
    });
    const loading = () =>
      slots.loading?.() ?? slots.fallback?.() ?? h("p", { role: "status" }, "Loading Jazz…");
    return () => {
      const state = snapshot.value;
      if (state.status === "ready" && state.client)
        return h(
          JazzClientProvider,
          {
            client: state.client,
            autoAttachDevTools: props.autoAttachDevTools,
          },
          { default: slots.default, fallback: loading },
        );
      if (state.status === "signed-out") return slots.signedOut?.() ?? null;
      if (state.status === "error")
        return (
          slots.error?.({ error: state.error, retry: auth.retry }) ??
          h("div", { role: "alert" }, [
            h("p", state.error?.message ?? "Jazz could not start"),
            h(
              "button",
              {
                type: "button",
                onClick: () => {
                  void auth.retry().catch(() => {});
                },
              },
              "Retry",
            ),
          ])
        );
      return loading();
    };
  },
});

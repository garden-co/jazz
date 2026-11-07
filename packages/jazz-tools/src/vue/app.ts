import {
  camelize,
  defineComponent,
  h,
  inject,
  nextTick,
  onMounted,
  onUnmounted,
  provide,
  shallowReadonly,
  shallowRef,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
import { createJazzAppOwner, type JazzAppSnapshot } from "../session/app.js";
import { createJazzClient, type JazzClientConfig } from "../session/create-jazz-session.js";
import type { JazzSessionActions } from "../session/state.js";
import type { AccountDbConfig } from "../accounts/context.js";
import type { JazzClient } from "./create-jazz-client.js";
import { JazzClientProvider, LegacyJazzProvider } from "./provider.js";

export type JazzProviderProps = Partial<JazzClientConfig> & {
  config?: AccountDbConfig;
  autoAttachDevTools?: boolean;
};
export type UseJazzAuth = {
  readonly snapshot: Readonly<ShallowRef<JazzAppSnapshot<JazzClient>>>;
  readonly sessionActions: JazzSessionActions;
  /**
   * Your auth changed: call this after your own sign-in, sign-out or account
   * switch. Jazz asks your backend who is signed in now and follows its
   * answer. Failures are represented in the snapshot.
   */
  reauthenticate(): Promise<void>;
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
    scope: String,
    serverUrl: { type: String as PropType<string | null>, default: undefined },
    initial: [String, Object] as PropType<JazzClientConfig["initial"]>,
    // Declared so `clear-on-sign-out` (bare or bound) reaches the session as
    // a boolean; left out, each keeps the session's own default.
    clearOnSignOut: { type: Boolean, default: undefined },
    linkGuestOnSignUp: { type: Boolean, default: undefined },
    autoAttachDevTools: { type: Boolean, default: true },
  },
  setup(props, { slots, attrs }) {
    if (props.config)
      return () =>
        h(
          LegacyJazzProvider,
          {
            ...attrs,
            config: props.config!,
            autoAttachDevTools: props.autoAttachDevTools,
          },
          slots,
        );
    // Vue passes undeclared attributes as written: `driver` stays `driver`,
    // but a kebab-case `log-level` must still reach the client as `logLevel`.
    const options: Record<string, unknown> = {};
    for (const [name, value] of Object.entries(attrs)) options[camelize(name)] = value;
    const config = {
      ...options,
      scope: props.scope,
      serverUrl: props.serverUrl,
      initial: props.initial,
      ...(props.clearOnSignOut === undefined ? {} : { clearOnSignOut: props.clearOnSignOut }),
      ...(props.linkGuestOnSignUp === undefined
        ? {}
        : { linkGuestOnSignUp: props.linkGuestOnSignUp }),
    } as JazzClientConfig;
    const app = createJazzAppOwner(config, createJazzClient, { start: false });
    const snapshot = shallowRef(app.getSnapshot());
    const lease = app.attachConsumer();
    const auth: UseJazzAuth = {
      snapshot: shallowReadonly(snapshot),
      sessionActions: app.sessionActions,
      reauthenticate: () => app.reauthenticate().catch(() => {}),
      logout: () => app.logout().catch(() => {}),
      retry: () => app.retry().catch(() => {}),
    };
    provide(AuthKey, auth);
    const stop = app.subscribe(() => {
      const observed = app.getSnapshot();
      snapshot.value = observed;
      // Vue flushes old descendants' unmount hooks before acknowledging.
      void nextTick(() => lease.acknowledge(observed));
    });
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

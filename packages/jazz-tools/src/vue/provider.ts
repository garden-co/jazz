import { SessionKey } from "./session-key.js";
import {
  defineComponent,
  inject,
  onUnmounted,
  provide,
  shallowRef,
  triggerRef,
  watch,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
import type { Db } from "../runtime/db.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";
import {
  refuseAutoAttachDevTools,
  type NoAutoAttachDevTools,
} from "../runtime/removed-provider-props.js";
import { toError } from "../runtime/util.js";

export type JazzClientContextValue = CreatedJazzClient;

export interface JazzClientProviderProps extends NoAutoAttachDevTools {
  client: CreatedJazzClient | Promise<CreatedJazzClient>;
}

const JazzContextKey: InjectionKey<ShallowRef<JazzClientContextValue | null>> = Symbol("jazz");

/**
 * Makes a Jazz client available to child components through Vue dependency injection.
 * Pass a pre-created client or a promise that resolves to one.
 */
export const JazzClientProvider = defineComponent({
  name: "JazzClientProvider",
  props: {
    client: {
      type: Object as PropType<JazzClientProviderProps["client"]>,
      required: true,
    },
    /** @internal Set by `<JazzProvider>` for its session's own client. */
    inSession: { type: Boolean, default: false },
  },
  setup(props, { slots, attrs }) {
    refuseAutoAttachDevTools("JazzClientProvider", attrs);
    // Its children read this client, not an enclosing <JazzProvider>'s session.
    if (!props.inSession) provide(SessionKey, undefined);
    const clientRef = shallowRef<JazzClientContextValue | null>(null);
    const errorRef = shallowRef<Error | null>(null);
    let runId = 0;

    provide(JazzContextKey, clientRef);

    watch(
      () => props.client,
      (nextClient, _previousClient, onCleanup) => {
        runId += 1;
        const activeRunId = runId;
        clientRef.value = null;
        errorRef.value = null;

        let cancelled = false;
        let stopSessionSync: (() => void) | null = null;
        onCleanup(() => {
          cancelled = true;
          stopSessionSync?.();
        });

        Promise.resolve(nextClient)
          .then((client) => {
            if (cancelled || activeRunId !== runId) {
              return;
            }

            clientRef.value = client;
            stopSessionSync = client.db.onAuthChanged(() => {
              if (cancelled || activeRunId !== runId) {
                return;
              }
              triggerRef(clientRef);
            });
          })
          .catch((reason) => {
            if (cancelled || activeRunId !== runId) {
              return;
            }

            errorRef.value = toError(reason);
          });
      },
      { immediate: true },
    );

    onUnmounted(() => {
      runId += 1;
      clientRef.value = null;
    });

    return () => {
      if (errorRef.value) {
        throw errorRef.value;
      }

      if (clientRef.value) {
        return slots.default?.();
      }

      return slots.fallback?.() ?? null;
    };
  },
});

/**
 * Get the current Jazz client, including the backing {@link Db}, session snapshot,
 * and shutdown helper.
 */
export function useJazzClient(): JazzClientContextValue {
  const ctx = inject(JazzContextKey, null);
  if (!ctx?.value) {
    throw new Error(
      "Jazz Vue composables must be used within <JazzProvider> or <JazzClientProvider>",
    );
  }
  return ctx.value;
}

/** @internal The nearest client's ref (triggered when its auth changes), or null. */
export function injectOptionalJazzClient(): ShallowRef<JazzClientContextValue | null> | null {
  return inject(JazzContextKey, null);
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb(): Db {
  return useJazzClient().db;
}

export { JazzProvider, type JazzProviderProps } from "./session.js";

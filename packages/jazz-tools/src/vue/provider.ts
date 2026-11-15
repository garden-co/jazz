import {
  computed,
  defineComponent,
  inject,
  onUnmounted,
  provide,
  shallowRef,
  triggerRef,
  watch,
  type ComputedRef,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
import type { PublicSession } from "../runtime/context.js";
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
  },
  setup(props, { slots, attrs }) {
    refuseAutoAttachDevTools("JazzClientProvider", attrs);
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

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb(): Db {
  return useJazzClient().db;
}

/**
 * Subscribe to the current Jazz {@link PublicSession}.
 * Returns a {@link ComputedRef} whose `.value` updates automatically as the user
 * logs in or out — use it in templates or computed properties.
 */
export function useSession(): ComputedRef<PublicSession | null> {
  const ctx = inject(JazzContextKey, null);
  if (!ctx) {
    throw new Error(
      "Jazz Vue composables must be used within <JazzProvider> or <JazzClientProvider>",
    );
  }
  return computed(() => ctx.value?.session ?? null);
}

export { JazzProvider, type JazzProviderProps } from "./session.js";

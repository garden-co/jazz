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
import type { Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";

export type JazzClientContextValue = CreatedJazzClient;

export interface JazzProviderProps {
  client: CreatedJazzClient | Promise<CreatedJazzClient>;
}

const JazzContextKey: InjectionKey<ShallowRef<JazzClientContextValue | null>> = Symbol("jazz");

/**
 * Makes a Jazz client available to child components through Vue dependency injection.
 * Pass a pre-created client or a promise that resolves to one.
 */
export const JazzProvider = defineComponent({
  name: "JazzProvider",
  props: {
    client: {
      type: Object as PropType<JazzProviderProps["client"]>,
      required: true,
    },
  },
  setup(props, { slots }) {
    const clientRef = shallowRef<JazzClientContextValue | null>(null);
    const errorRef = shallowRef<Error | null>(null);
    let runId = 0;
    let resolvedClient: CreatedJazzClient | null = null;

    provide(JazzContextKey, clientRef);

    watch(
      () => props.client,
      (nextClient, _previousClient, onCleanup) => {
        runId += 1;
        const activeRunId = runId;
        const previousClient = resolvedClient;
        resolvedClient = null;
        clientRef.value = null;
        errorRef.value = null;

        if (previousClient) {
          void previousClient.shutdown();
        }

        let cancelled = false;
        let stopSessionSync: (() => void) | null = null;
        onCleanup(() => {
          cancelled = true;
          stopSessionSync?.();
        });

        Promise.resolve(nextClient)
          .then((client) => {
            if (cancelled || activeRunId !== runId) {
              void client.shutdown();
              return;
            }

            resolvedClient = client;
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

            errorRef.value = reason instanceof Error ? reason : new Error(String(reason));
          });
      },
      { immediate: true },
    );

    onUnmounted(() => {
      runId += 1;
      const activeClient = resolvedClient;
      resolvedClient = null;
      clientRef.value = null;

      if (activeClient) {
        void activeClient.shutdown();
      }
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
 * Get the current Jazz client, including the backing {@link Db}, subscription manager,
 * session snapshot, and shutdown helper.
 */
export function useJazzClient(): JazzClientContextValue {
  const ctx = inject(JazzContextKey, null);
  if (!ctx?.value) {
    throw new Error("Jazz Vue composables must be used within <JazzProvider>");
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
 * Get the current Jazz {@link Session}, including the user's id, claims and auth mode.
 */
export function useSession(): Session | null {
  return useJazzClient().session ?? null;
}

import {
  defineComponent,
  inject,
  onUnmounted,
  provide,
  shallowRef,
  watch,
  type InjectionKey,
  type PropType,
  type ShallowRef,
} from "vue";
import type { Session } from "../runtime/context.js";
import type { Db } from "../runtime/db.js";
import type { JazzClient as CreatedJazzClient } from "./create-jazz-client.js";

export interface JazzClientContextValue {
  db: Db;
  manager: CreatedJazzClient["manager"];
  session: Session | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export interface JazzProviderProps {
  client: CreatedJazzClient | Promise<CreatedJazzClient>;
}

const JazzContextKey: InjectionKey<ShallowRef<JazzClientContextValue | null>> = Symbol("jazz");

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
        onCleanup(() => {
          cancelled = true;
        });

        Promise.resolve(nextClient)
          .then((client) => {
            if (cancelled || activeRunId !== runId) {
              void client.shutdown();
              return;
            }

            resolvedClient = client;
            clientRef.value = client;
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

export function useJazzClient(): JazzClientContextValue {
  const ctx = inject(JazzContextKey, null);
  if (!ctx?.value) {
    throw new Error("Jazz Vue composables must be used within <JazzProvider>");
  }
  return ctx.value;
}

export function useDb(): Db {
  return useJazzClient().db;
}

export function useSession(): Session | null {
  return useJazzClient().session ?? null;
}

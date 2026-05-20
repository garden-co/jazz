import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";
import { browserAuthSecretStore, type AuthSecretStore } from "../runtime/auth-secret-store.js";

const listenersByStore = new WeakMap<AuthSecretStore, Set<() => void>>();

function getListeners(store: AuthSecretStore): Set<() => void> {
  let listeners = listenersByStore.get(store);
  if (!listeners) {
    listeners = new Set();
    listenersByStore.set(store, listeners);
  }
  return listeners;
}

export interface UseLocalFirstAuth {
  secret: Ref<string | null>;
  isLoading: Ref<boolean>;
  login: (secret: string) => Promise<void>;
  signOut: () => Promise<void>;
}

/**
 * Reactive local-first auth secret for Vue. Mirrors `useLocalFirstAuth()`
 * from `jazz-tools/react` and the `LocalFirstAuth` class from
 * `jazz-tools/svelte`. Call inside a Vue `<script setup>` block or any
 * `effectScope`.
 *
 * The secret store is only read on the client (guarded by `typeof window`),
 * so calling this composable during Vue SSR setup is a no-op — the server
 * render sees `secret: null, isLoading: true` and never touches
 * `localStorage`.
 *
 * `login` and `signOut` notify every live consumer backed by the same store,
 * so a sign-out button anywhere in the tree invalidates a provider mounted
 * higher up without a manual reload.
 *
 * ```vue
 * <script setup lang="ts">
 * import { computed } from 'vue';
 * import { useLocalFirstAuth, createJazzClient, JazzProvider } from 'jazz-tools/vue';
 * import TodoList from './TodoList.vue';
 *
 * const { secret, isLoading } = useLocalFirstAuth();
 *
 * const client = computed(() =>
 *   !isLoading.value && secret.value
 *     ? createJazzClient({ appId: '<your-app-id>', secret: secret.value })
 *     : null,
 * );
 * </script>
 *
 * <template>
 *   <JazzProvider v-if="client" :client="client">
 *     <TodoList />
 *   </JazzProvider>
 * </template>
 * ```
 *
 * @param store - optional {@link AuthSecretStore} override. Defaults to the
 *   shared {@link browserAuthSecretStore} singleton. Pass a custom store to
 *   isolate secrets per app, user, or session, or to swap in an alternative
 *   storage backend.
 */
export function useLocalFirstAuth(
  store: AuthSecretStore = browserAuthSecretStore,
): UseLocalFirstAuth {
  const secret = ref<string | null>(null);
  const isLoading = ref(true);
  const listeners = getListeners(store);

  let cancelled = false;
  let latestCallId = 0;

  const refetch = () => {
    const callId = ++latestCallId;
    const stale = () => cancelled || callId !== latestCallId;
    const onError = (err: unknown) => {
      if (stale()) return;
      console.warn("[useLocalFirstAuth] secret store failed:", err);
      secret.value = null;
      isLoading.value = false;
    };

    isLoading.value = true;
    try {
      store
        .getOrCreateSecret()
        .then((resolved) => {
          if (stale()) return;
          secret.value = resolved;
          isLoading.value = false;
        })
        .catch(onError);
    } catch (err) {
      onError(err);
    }
  };

  if (typeof window !== "undefined") {
    refetch();
    listeners.add(refetch);
  }

  if (getCurrentScope()) {
    onScopeDispose(() => {
      cancelled = true;
      listeners.delete(refetch);
    });
  } else if (typeof window !== "undefined") {
    console.warn(
      "[useLocalFirstAuth] called outside an active effect scope; the secret-store listener will leak until the page unloads. " +
        "Call this composable inside <script setup> or effectScope().",
    );
  }

  return {
    secret,
    isLoading,
    async login(s: string) {
      try {
        await store.saveSecret(s);
      } finally {
        for (const fn of listeners) fn();
      }
    },
    async signOut() {
      try {
        await store.clearSecret();
      } finally {
        for (const fn of listeners) fn();
      }
    },
  };
}

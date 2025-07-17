import { JazzClerkAuth, type MinimalClerkClient } from "jazz-tools";
import { computed, markRaw, onMounted, onUnmounted } from "vue";
import { useAuthSecretStorage, useJazzContext } from "../composables.js";

export function useClerkAuth(clerk: MinimalClerkClient) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context.value) {
    throw new Error("Clerk auth is not supported in guest mode");
  }

  // Create auth method similar to React's useMemo pattern
  const authMethod = computed(() => {
    return markRaw(
      new JazzClerkAuth(context.value.authenticate, authSecretStorage),
    );
  });

  onMounted(() => {
    const cleanup = authMethod.value.registerListener(clerk) as
      | (() => void)
      | void;

    onUnmounted(() => {
      // Clerk's addListener returns a cleanup function, but the type says void
      // Handle both cases for type safety
      if (typeof cleanup === "function") {
        cleanup();
      }
    });
  });

  return authMethod.value;
}

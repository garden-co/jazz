import { onUnmounted, ref } from "vue";
import { useAuthSecretStorage } from "../composables.js";

export function useIsAuthenticated() {
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = ref(authSecretStorage.isAuthenticated);

  const handleUpdate = () => {
    isAuthenticated.value = authSecretStorage.isAuthenticated;
  };

  // Set up the listener immediately, not waiting for onMounted
  // This ensures we catch auth state changes that happen before mounting
  const cleanup = authSecretStorage.onUpdate(handleUpdate);

  onUnmounted(() => {
    cleanup();
  });

  return isAuthenticated;
}

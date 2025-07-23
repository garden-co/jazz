import { DemoAuth } from "jazz-tools";
import { computed, markRaw, ref, watch } from "vue";
import { useAuthSecretStorage, useJazzContext } from "../composables.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

export function useDemoAuth() {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context.value) {
    throw new Error("Demo auth is not supported in guest mode");
  }

  const authMethod = computed(() =>
    markRaw(new DemoAuth(context.value.authenticate, authSecretStorage)),
  );

  const existingUsers = ref<string[]>([]);
  const isAuthenticated = useIsAuthenticated();

  watch(
    authMethod,
    () => {
      authMethod.value.getExistingUsers().then((users) => {
        existingUsers.value = users;
      });
    },
    { immediate: true },
  );

  function handleSignUp(username: string) {
    return authMethod.value.signUp(username).then(() => {
      existingUsers.value = existingUsers.value.concat([username]);
    });
  }

  return computed(() => ({
    state: isAuthenticated.value ? "signedIn" : "anonymous",
    logIn: authMethod.value.logIn,
    signUp: handleSignUp,
    existingUsers: existingUsers.value,
  }));
}

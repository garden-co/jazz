import { createEffect, createSignal, onCleanup } from "solid-js";
import { useAuthSecretStorage } from "../context/jazz.js";

export function useIsAuthenticated() {
  const authSecretStorage = useAuthSecretStorage();
  const [authenticated, setAuthenticated] = createSignal(
    authSecretStorage().isAuthenticated,
  );

  createEffect(() => {
    const off = authSecretStorage().onUpdate(setAuthenticated);

    onCleanup(() => {
      off();
    });
  });

  return authenticated;
}

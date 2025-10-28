import { BrowserPasskeyAuth } from "jazz-tools/browser";
import { getAuthSecretStorage, getJazzContext } from "../jazz.svelte.js";
import { useIsAuthenticated } from "./useIsAuthenticated.svelte.js";

export type PasskeyAuth = {
  current: BrowserPasskeyAuth;
  state: "anonymous" | "signedIn";
};

/** @category Auth Providers */
export function usePasskeyAuth({
  appName,
  appHostname,
}: {
  appName: string;
  appHostname?: string;
}): PasskeyAuth {
  const context = getJazzContext();
  const authSecretStorage = getAuthSecretStorage();
  const auth = new BrowserPasskeyAuth(
    context.current.node.crypto,
    context.current.authenticate,
    authSecretStorage,
    appName,
    appHostname,
  );

  if ("guest" in context.current) {
    throw new Error("Passkey auth is not supported in guest mode");
  }

  const isAuthenticated = useIsAuthenticated();

  const state = $derived(isAuthenticated.current ? "signedIn" : "anonymous");

  return {
    current: auth,
    get state() {
      return state;
    },
  };
}

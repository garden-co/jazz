import { untrack } from "svelte";
import { getAuthSecretStorage, getJazzContext } from "../jazz.svelte.js";
import { useIsAuthenticated } from "./useIsAuthenticated.svelte.js";
import { PassphraseAuth } from "jazz-tools";
import { createSubscriber } from "svelte/reactivity";

/** @category Auth Providers */
export function usePassphraseAuth({
  wordlist,
}: {
  wordlist: string[];
}) {
  const context = getJazzContext();
  const authSecretStorage = getAuthSecretStorage();
  const auth = new PassphraseAuth(
    context.current.node.crypto,
    context.current.authenticate,
    context.current.register,
    authSecretStorage,
    wordlist
  );

  auth.loadCurrentAccountPassphrase();

  const subscribe = createSubscriber((update) => {
    const off = auth.subscribe(update)

    return () => off()
  })

  const isAuthenticated = useIsAuthenticated();

  const state = $derived(isAuthenticated.current ? "signedIn" : "anonymous");

  return {
    logIn: auth.logIn,
    signUp: auth.signUp,
    registerNewAccount: auth.registerNewAccount,
    generateRandomPassphrase: auth.generateRandomPassphrase,
    get passphrase() {
      subscribe();
  
      return auth.passphrase;
    },
    get state() {
      return state;
    },
  };
}

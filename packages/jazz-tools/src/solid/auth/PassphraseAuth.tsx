import { PassphraseAuth } from "jazz-tools";
import { createEffect, createMemo, createSignal, onCleanup } from "solid-js";
import { useAuthSecretStorage, useJazzContext } from "../jazz.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

/** @category Auth Providers */
export function usePassphraseAuth({
  wordlist,
}: {
  wordlist: string[];
}) {
  const jazz = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = useIsAuthenticated();

  const state = () => (isAuthenticated() ? "signedIn" : "anonymous");

  const auth = createMemo(() => {
    const pAuth = new PassphraseAuth(
      jazz().node.crypto,
      jazz().authenticate,
      jazz().register,
      authSecretStorage(),
      wordlist,
    );

    void pAuth.loadCurrentAccountPassphrase();

    return pAuth;
  });

  const [passphrase, setPassphrase] = createSignal(auth().passphrase);

  createEffect(() => {
    const off = auth().subscribe(() => {
      setPassphrase(auth().passphrase);
    });

    onCleanup(() => {
      off();
    });
  });

  return {
    logIn: auth().logIn,
    signUp: auth().signUp,
    registerNewAccount: auth().registerNewAccount,
    generateRandomPassphrase: auth().generateRandomPassphrase,
    passphrase,
    state,
  };
}

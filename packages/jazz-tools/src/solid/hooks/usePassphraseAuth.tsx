import { PassphraseAuth } from "jazz-tools";
import {
  Accessor,
  createEffect,
  createMemo,
  createSignal,
  onCleanup,
} from "solid-js";
import { useAuthSecretStorage, useJazzContext } from "../context/jazz.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

type PassphraseAuthParams = {
  readonly wordlist: string[];
};

/**
 * `usePassphraseAuth` hook provides a `JazzAuth` object for passphrase authentication.
 *
 * @example
 * ```ts
 * const auth = usePassphraseAuth(() => ({ appName, appHostname, wordlist }));
 * ```
 *
 * @category Auth Providers
 */
export const usePassphraseAuth = (params: Accessor<PassphraseAuthParams>) => {
  const jazz = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = useIsAuthenticated();

  const state = () => (isAuthenticated() ? "signedIn" : "anonymous");

  const auth = createMemo(() => {
    if ("guest" in jazz()) {
      throw new Error("Passphrase auth is not supported in guest mode");
    }

    const pAuth = new PassphraseAuth(
      jazz().node.crypto,
      jazz().authenticate,
      jazz().register,
      authSecretStorage(),
      params().wordlist,
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

  const logIn = (passphrase: string) => auth().logIn(passphrase);
  const signUp = (name?: string) => auth().signUp(name);
  const registerNewAccount = (passphrase: string, name: string) =>
    auth().registerNewAccount(passphrase, name);
  const generateRandomPassphrase = () => auth().generateRandomPassphrase();

  return {
    logIn,
    signUp,
    registerNewAccount,
    generateRandomPassphrase,
    passphrase,
    state,
  } as const;
};

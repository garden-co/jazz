const apiKey = "";
import type {
  AccountClass,
  Account,
  CoValueFromRaw,
  AnyAccountSchema,
} from "jazz-tools";
import {
  type JazzContextManagerProps,
  JazzBrowserContextManager,
} from "jazz-tools/browser";

export async function createVanillaJazzApp<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(opts: Pick<JazzContextManagerProps<S>, "sync" | "AccountSchema">) {
  const contextManager = new JazzBrowserContextManager<S>();

  await contextManager.createContext({
    guestMode: false,
    ...opts,
  });

  function getCurrentAccount() {
    const context = contextManager.getCurrentValue();
    if (!context || !("me" in context)) {
      throw new Error("");
    }

    return context.me;
  }

  return {
    me: getCurrentAccount(),
    getCurrentAccount,
    logOut: contextManager.logOut,
    authSecretStorage: contextManager.getAuthSecretStorage(),
    authenticate: contextManager.authenticate,
    register: contextManager.register,
  };
}

const { me, authSecretStorage, authenticate, register } =
  await createVanillaJazzApp({
    sync: {
      peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      when: "always",
    },
  });
// #region PassphraseAuth
import { PassphraseAuth } from "jazz-tools";
import { wordlist } from "./wordlist";
const crypto = me.$jazz.localNode.crypto;

// `authenticate` and `register` are methods provided by the Context Manager, and should be returned from your `createVanillaJazzApp` function
const auth = new PassphraseAuth(
  crypto,
  authenticate,
  register,
  authSecretStorage,
  wordlist,
);
// #endregion

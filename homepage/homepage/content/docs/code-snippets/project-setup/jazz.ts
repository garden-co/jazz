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
  };
}

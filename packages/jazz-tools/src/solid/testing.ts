import { Account, AnonymousJazzAgent, AuthSecretStorage } from "jazz-tools";
import { TestJazzContextManager } from "jazz-tools/testing";
import { type Accessor, createMemo } from "solid-js";
import { JAZZ_AUTH_CTX, JAZZ_CTX, type JazzContextValue } from "./jazz.js";

export function createJazzTestContext<Acc extends Account>(
  opts: {
    account?: Acc | { guest: AnonymousJazzAgent };
    isAuthenticated?: boolean;
  } = {},
) {
  const ctx = new Map<
    typeof JAZZ_CTX | typeof JAZZ_AUTH_CTX,
    JazzContextValue<Acc> | Accessor<AuthSecretStorage>
  >();

  const account = () => opts.account ?? (Account.getMe() as Acc);

  const value = createMemo(() =>
    TestJazzContextManager.fromAccountOrGuest<Acc>(account(), {
      isAuthenticated: opts.isAuthenticated,
    }),
  );

  const jazzContextValue = () => value().getCurrentValue();
  const authStorageValue = () => value().getAuthSecretStorage();

  ctx.set(JAZZ_AUTH_CTX, authStorageValue);
  ctx.set(JAZZ_CTX, { current: jazzContextValue });

  return ctx;
}

export {
  createJazzTestAccount,
  createJazzTestGuest,
  linkAccounts,
  setActiveAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";

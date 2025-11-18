import { Account, AnonymousJazzAgent, AuthSecretStorage } from "jazz-tools";
import { TestJazzContextManager } from "jazz-tools/testing";
import { JAZZ_AUTH_CTX, JAZZ_CTX, type JazzContext } from "./jazz.svelte.js";

export function createJazzTestContext<Acc extends Account>(
  opts: {
    account?: Acc | { guest: AnonymousJazzAgent };
    isAuthenticated?: boolean;
  } = {},
) {
  const ctx = new Map<
    typeof JAZZ_CTX | typeof JAZZ_AUTH_CTX,
    JazzContext<Acc> | AuthSecretStorage
  >();
  const account = opts.account ?? (Account.getMe() as Acc);

  const value = TestJazzContextManager.fromAccountOrGuest<Acc>(account, {
    isAuthenticated: opts.isAuthenticated,
  });

  ctx.set(JAZZ_AUTH_CTX, value.getAuthSecretStorage());

  if ("guest" in account) {
    ctx.set(JAZZ_CTX, {
      current: value.getCurrentValue(),
    });
  } else {
    ctx.set(JAZZ_CTX, {
      current: value.getCurrentValue(),
    });
  }

  return ctx;
}

export {
  createJazzTestAccount,
  createJazzTestGuest,
  linkAccounts,
  setActiveAccount,
  setupJazzTestSync,
  MockConnectionStatus,
} from "jazz-tools/testing";

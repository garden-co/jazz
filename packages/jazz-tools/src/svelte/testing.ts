import { type Account, AnonymousJazzAgent, AuthSecretStorage, co } from "jazz-tools";
import { TestJazzContextManager } from "jazz-tools/testing";
import { JAZZ_AUTH_CTX, JAZZ_CTX, type JazzContext } from "./jazz.svelte.js";
import { AccountSchema } from "../tools/implementation/zodSchema/schemaTypes/AccountSchema.js";
import { Loaded } from "../tools/internal.js";

export function createJazzTestContext<Acc extends AccountSchema>(
  opts: {
    account?: Loaded<Acc> | { guest: AnonymousJazzAgent };
    isAuthenticated?: boolean;
  } = {},
) {
  const ctx = new Map<
    typeof JAZZ_CTX | typeof JAZZ_AUTH_CTX,
    JazzContext<Acc> | AuthSecretStorage
  >();
  const account = opts.account ?? (co.account().getMe() as Loaded<Acc>);

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

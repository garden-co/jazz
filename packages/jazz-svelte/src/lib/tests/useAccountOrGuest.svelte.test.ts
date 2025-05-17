import { render } from "@testing-library/svelte";
import { Account, AnonymousJazzAgent, CoMap, coField, type RefsToResolve } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { useAccountOrGuest, type RegisteredAccount } from "../index.js";
import { createJazzTestAccount, createJazzTestContext, createJazzTestGuest } from "../testing.js";
import UseAccountOrGuest from "./components/useAccountOrGuest.svelte";

function setup(options: {
  account: RegisteredAccount | { guest: AnonymousJazzAgent };
  depth?: RefsToResolve<RegisteredAccount>;
}) {
  const result = { current: undefined } as { current: ReturnType<typeof useAccountOrGuest> | undefined };

    render(UseAccountOrGuest, {
      context: createJazzTestContext({ account: options.account }),
      props: {
        depth: options.depth ?? {},
        setResult: (value) => {
          result.current = value
        },
      },
    });

  return result;
}

declare module "../jazz.svelte.js" {
  interface Register {
    Account: AccountSchema;
  }
}

class AccountRoot extends CoMap {
  value = coField.string;
}

class AccountSchema extends Account {
  root = coField.ref(AccountRoot);

  migrate() {
    if (!this._refs.root) {
      this.root = AccountRoot.create({ value: "123" }, { owner: this });
    }
  }
}

describe("useAccountOrGuest", () => {
  it("should return the correct value", async () => {
    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const result = setup({
      account,
    });

      expect(result.current?.me).toEqual(account);
  });

  it("should return the guest agent if the account is a guest", async () => {
    const account = await createJazzTestGuest();

    const result = setup({
      account,
    });

      expect(result.current?.me).toEqual(account.guest);
  });

  it("should load nested values if requested", async () => {
    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const result = setup({
      account,
      depth: {
        root: {}
      }
    });

      // @ts-expect-error Skipping the guest check
      expect(result.current?.me?.root?.value).toBe("123");
  });
});
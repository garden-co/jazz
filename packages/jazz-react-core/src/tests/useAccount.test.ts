// @vitest-environment happy-dom

import { Account, CoMap, co } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { createUseAccountHooks } from "../hooks.js";
import { createJazzTestAccount } from "../testing.js";
import { renderHook } from "./testUtils.js";

const { useAccount } = createUseAccountHooks<Account>();

describe("useAccount", () => {
  it("should return the correct value", async () => {
    const account = await createJazzTestAccount();

    const { result } = renderHook(() => useAccount(), {
      account,
    });

    expect(result.current?.me).toEqual(account);
  });

  it("should load nested values if requested", async () => {
    class AccountRoot extends CoMap {
      value = co.string;
    }

    class AccountSchema extends Account {
      root = co.ref(AccountRoot);

      migrate() {
        if (!this._refs.root) {
          this.root = AccountRoot.create({ value: "123" }, { owner: this });
        }
      }
    }

    const { useAccount } = createUseAccountHooks<AccountSchema>();

    const account = await createJazzTestAccount({ AccountSchema });

    const { result } = renderHook(
      () =>
        useAccount({
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    expect(result.current?.me?.root?.value).toBe("123");
  });
});

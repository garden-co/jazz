// @vitest-environment happy-dom

import { Account, CoMap, co } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { createUseAccountHooks } from "../index.js";
import { createJazzTestAccount, createJazzTestGuest } from "../testing.js";
import { renderHook } from "./testUtils.js";

const { useAccountOrGuest } = createUseAccountHooks<Account>();

describe("useAccountOrGuest", () => {
  it("should return the correct me value", async () => {
    const account = await createJazzTestAccount();

    const { result } = renderHook(() => useAccountOrGuest(), {
      account,
    });

    expect(result.current?.me).toEqual(account);
  });

  it("should return the guest agent if the account is a guest", async () => {
    const account = await createJazzTestGuest();

    const { result } = renderHook(() => useAccountOrGuest(), {
      account,
    });

    expect(result.current?.me).toBe(account.guest);
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

    const account = await createJazzTestAccount({ AccountSchema });

    const { result } = renderHook(
      () =>
        useAccountOrGuest({
          root: {},
        }),
      {
        account,
      },
    );

    // @ts-expect-error
    expect(result.current.me?.root?.value).toBe("123");
  });

  it("should not load nested values if the account is a guest", async () => {
    const account = await createJazzTestGuest();

    const { result } = renderHook(
      () =>
        useAccountOrGuest({
          root: {},
        }),
      {
        account,
      },
    );

    expect(result.current.me).toBe(account.guest);
  });
});

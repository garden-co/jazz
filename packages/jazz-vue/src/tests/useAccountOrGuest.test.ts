// @vitest-environment happy-dom

import { Account, CoMap, coField } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { useAccountOrGuest } from "../composables.js";
import { createJazzTestAccount, createJazzTestGuest } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

describe("useAccountOrGuest", () => {
  it("should return the correct me value", async () => {
    const account = await createJazzTestAccount();

    const [result] = withJazzTestSetup(() => useAccountOrGuest(), {
      account,
    });

    expect(result.me.value).toEqual(account);
  });

  it("should return the guest agent if the account is a guest", async () => {
    const account = await createJazzTestGuest();

    const [result] = withJazzTestSetup(() => useAccountOrGuest(), {
      account,
    });

    expect(result.me.value).toBe(account.guest);
  });

  it("should load nested values if requested", async () => {
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

    const account = await createJazzTestAccount({ AccountSchema });

    const [result] = withJazzTestSetup(
      () =>
        useAccountOrGuest({
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    // @ts-expect-error
    expect(result.me.value?.root?.value).toBe("123");
  });

  it("should not load nested values if the account is a guest", async () => {
    const account = await createJazzTestGuest();

    const [result] = withJazzTestSetup(
      () =>
        useAccountOrGuest({
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    expect(result.me.value).toBe(account.guest);
  });
});

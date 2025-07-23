// @vitest-environment happy-dom

import { Account, CoMap, coField } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { useAccount } from "../composables.js";
import { createJazzTestAccount, createJazzTestGuest } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

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

declare module "../provider" {
  interface Register {
    Account: AccountSchema;
  }
}

describe("useAccount", () => {
  it("should return the correct value", async () => {
    const account = await createJazzTestAccount();

    const [result] = withJazzTestSetup(() => useAccount(), {
      account,
    });

    expect(result.me.value).toEqual(account);
  });

  it("should load nested values if requested", async () => {
    const account = await createJazzTestAccount({ AccountSchema });

    const [result] = withJazzTestSetup(
      () =>
        useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    expect((result.me.value?.root as AccountRoot)?.value).toBe("123");
  });

  it("should handle guest mode correctly", async () => {
    const guestAccount = await createJazzTestGuest();

    const [result] = withJazzTestSetup(() => useAccount(), {
      account: guestAccount,
    });

    // In guest mode, me should be null and agent should be the guest
    expect(result.me.value).toBe(null);
    expect(result.agent._type).toBe("Anonymous");
    expect(typeof result.logOut).toBe("function");
  });
});

// @vitest-environment happy-dom

import { Account, CoMap, co } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { useAccount } from "../composables.js";
import { createJazzTestAccount } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

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
        useAccount({
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    expect(result.me.value?.root?.value).toBe("123");
  });
});

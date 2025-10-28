// @vitest-environment happy-dom

import { Group, co, z } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { useAccount } from "../composables.js";
import { createJazzTestAccount, createJazzTestGuest } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";

const AccountRoot = co.map({
  value: z.string(),
});

const AccountProfile = co.map({
  name: z.string(),
});

const AccountSchema = co
  .account({
    root: AccountRoot,
    profile: AccountProfile,
  })
  .withMigration((account) => {
    if (!account.root) {
      account.$jazz.set("root", { value: "123" });
    }
    if (!account.profile) {
      // Profile must be owned by a Group, not the account itself
      const group = Group.create();
      account.$jazz.set("profile", { name: "Test User" });
    }
  });

describe("useAccount", () => {
  it("should return the correct value", async () => {
    const account = await createJazzTestAccount();

    const [result] = withJazzTestSetup(() => useAccount(), {
      account,
    });

    expect(result.me.value).toEqual(account);
  });

  it("should handle guest mode correctly", async () => {
    const guestAccount = await createJazzTestGuest();

    const [result] = withJazzTestSetup(() => useAccount(), {
      account: guestAccount,
    });

    // In guest mode, me should be null and agent should be the guest
    expect(result.me.value).toBe(null);
    expect(result.agent.$type$).toBe("Anonymous");
    expect(typeof result.logOut).toBe("function");
  });
});

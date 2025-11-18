import { co, CoPlainText } from "jazz-tools";
import { assertLoaded, createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { beforeEach, describe, expectTypeOf, test } from "vitest";
import { AccountCoState } from "../jazz.class.svelte";

describe("AccountCoState", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("should use the schema's resolve query if no resolve query is provided", async () => {
    const AccountWithProfile = co.account().resolved({ profile: true });

    const loadedAccount = new AccountCoState(AccountWithProfile);

    assertLoaded(loadedAccount.current);
    expectTypeOf<typeof loadedAccount.current.profile.name>().toEqualTypeOf<string>();
  });
});

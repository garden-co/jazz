// #region SetupJazzTestSync
import { co, z } from "jazz-tools";
import { beforeEach, describe, expect, test } from "vitest";
import {
  createJazzTestAccount,
  runWithoutActiveAccount,
  setActiveAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
const MyAccountSchema = co.account({
  profile: co.profile(),
  root: co.map({}),
});

// #region SetupJazzTestSync
describe("My app's tests", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
  });

  test("I can create a test account", async () => {
    // See below for details on createJazzTestAccount()
    const account1 = await createJazzTestAccount({
      AccountSchema: MyAccountSchema,
      isCurrentActiveAccount: true,
    });
    expect(account1).not.toBeUndefined();
    // ...
  });
});
// #endregion

// #region CreateTestAccount
const account = await createJazzTestAccount({
  AccountSchema: MyAccountSchema,
  isCurrentActiveAccount: true,
  creationProps: {},
});
// #endregion

// #region CurrentlyActive
// @ts-expect-error redeclaring
const account1 = await createJazzTestAccount({
  isCurrentActiveAccount: true,
});

// @ts-expect-error redeclaring
const group1 = co.group().create(); // Group is owned by account1;

// @ts-expect-error redeclaring
const account2 = await createJazzTestAccount();
const group2 = co.group().create(); // Group is still owned by account1;
// #endregion

const MyMap = co.map({ text: z.string() });

// #region SetActiveAccount
// @ts-expect-error redeclaring
const account1 = await createJazzTestAccount({
  isCurrentActiveAccount: true,
});
// @ts-expect-error redeclaring
const account2 = await createJazzTestAccount();
// @ts-expect-error redeclaring
const group1 = co.group().create(); // Group is owned by account1;
group1.addMember(account2, "reader");

const myMap = MyMap.create(
  {
    text: "Created by account1",
  },
  { owner: group1 },
);
const myMapId = myMap.$jazz.id;

setActiveAccount(account2);
// myMap is still loaded as account1, so we need to load again as account2
const myMapFromAccount2 = await MyMap.load(myMapId);

if (myMapFromAccount2.$isLoaded) {
  expect(myMapFromAccount2.text).toBe("Created by account1");
  expect(() =>
    myMapFromAccount2.$jazz.set("text", "Updated by account2"),
  ).toThrow();
}
// #endregion

// #region RunWithoutAccount
// @ts-expect-error redeclaring
const account1 = await createJazzTestAccount({
  isCurrentActiveAccount: true,
});

runWithoutActiveAccount(() => {
  expect(() => co.group().create()).toThrow(); // can't create new group
});
// #endregion

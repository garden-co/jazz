/**
 * Learn about schemas here:
 * https://jazz.tools/docs/react/schemas/covalues
 */

import { Group, co, z } from "jazz-tools";

/** The account profile is an app-specific per-user public `CoMap`
 *  where you can store top-level objects for that user */
export const JazzProfile = co.profile({
  /**
   * Learn about CoValue field/item types here:
   * https://jazz.tools/docs/react/schemas/covalues#covalue-fielditem-types
   */
  firstName: z.string(),

  // Add public fields here
});

/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const AccountRoot = co.map({
  dateOfBirth: z.date(),
});

export function getUserAge(root: co.loaded<typeof AccountRoot> | undefined) {
  if (!root) return null;
  return new Date().getFullYear() - root.dateOfBirth.getFullYear();
}

export const JazzAccount = co
  .account({
    profile: JazzProfile,
    root: AccountRoot,
  })
  .withMigration(async (account) => {
    /** The account migration is run on account creation and on every log-in.
     *  You can use it to set up the account root and any other initial CoValues you need.
     */
    if (account.root === undefined) {
      const group = Group.create();

      account.root = AccountRoot.create(
        {
          dateOfBirth: new Date("1/1/1990"),
        },
        group,
      );
    }

    if (account.profile === undefined) {
      const group = Group.create();
      group.addMember("everyone", "reader"); // The profile info is visible to everyone

      account.profile = JazzProfile.create(
        {
          name: "Anonymous user",
          firstName: "",
        },
        group,
      );
    }
  });

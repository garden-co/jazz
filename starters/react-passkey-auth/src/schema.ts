/**
 * Learn about schemas here:
 * https://jazz.tools/docs/react/schemas/covalues
 */

import { Account, CoMap, Group, Profile, coField } from "jazz-tools";

/** The account profile is an app-specific per-user public `CoMap`
 *  where you can store top-level objects for that user */
export class JazzProfile extends Profile {
  /**
   * Learn about CoValue field/item types here:
   * https://jazz.tools/docs/react/schemas/covalues#covalue-fielditem-types
   */
  firstName = coField.string;

  // Add public fields here
}

/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export class AccountRoot extends CoMap {
  dateOfBirth = coField.Date;

  // Add private fields here

  static age(root: AccountRoot) {
    if (!root?.dateOfBirth) return null;

    return new Date().getFullYear() - root.dateOfBirth.getFullYear();
  }
}

export class JazzAccount extends Account {
  profile = coField.ref(JazzProfile);
  root = coField.ref(AccountRoot);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate(this: JazzAccount) {
    if (this.root === undefined) {
      const group = Group.create();

      this.root = AccountRoot.create(
        {
          dateOfBirth: new Date("1/1/1990"),
        },
        group,
      );
    }

    if (this.profile === undefined) {
      const group = Group.create();
      group.addMember("everyone", "reader"); // The profile info is visible to everyone

      this.profile = JazzProfile.create(
        {
          name: "Anonymous user",
          firstName: "",
        },
        group,
      );
    }
  }
}

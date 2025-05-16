/**
 * Learn about schemas here:
 * https://jazz.tools/docs/react/schemas/covalues
 */

import { Account, Group, Profile } from "jazz-tools";

export class JazzAccount extends Account {
  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate(this: JazzAccount) {
    if (this.profile === undefined) {
      const group = Group.create();
      group.addMember("everyone", "reader"); // The profile info is visible to everyone

      this.profile = Profile.create({ name: "supercoolusername" }, group);
    }
  }
}

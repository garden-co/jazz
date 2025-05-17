import { Account, FileStream, Profile, coField } from "jazz-tools";

export class JazzProfile extends Profile {
  file = coField.ref(FileStream, { optional: true });
}

export class JazzAccount extends Account {
  profile = coField.ref(JazzProfile);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate(this: JazzAccount) {}
}

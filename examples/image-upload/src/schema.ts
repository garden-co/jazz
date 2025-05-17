import {
  Account,
  ImageDefinition,
  Profile,
  coField,
  zodSchemaToCoSchema,
} from "jazz-tools";

export class JazzProfile extends Profile {
  image = coField.ref(zodSchemaToCoSchema(ImageDefinition), { optional: true });
}

export class JazzAccount extends Account {
  profile = coField.ref(JazzProfile);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate(this: JazzAccount) {}
}

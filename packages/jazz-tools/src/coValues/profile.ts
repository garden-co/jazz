import { CoID } from "cojson";
import {
  Account,
  CoMap,
  CoMapInit,
  CoValueClass,
  Group,
  InboxInvite,
  InboxRoot,
  Simplify,
  coField,
} from "../internal.js";

/** @category Identity & Permissions */
export class Profile extends CoMap {
  name = coField.string;
  inbox? = coField.optional.json<CoID<InboxRoot>>();
  inboxInvite? = coField.optional.json<InboxInvite>();

  /**
   * Creates a new profile with the given initial values and owner.
   *
   * The owner (a Group) determines access rights to the Profile.
   *
   * @category Creation
   */
  static override create<M extends CoMap>(
    this: CoValueClass<M>,
    init: Simplify<CoMapInit<M>>,
    options?:
      | {
          owner: Group;
        }
      | Group,
  ) {
    const owner =
      options !== undefined && "owner" in options ? options.owner : options;

    // We add some guardrails to ensure that the owner of a profile is a group
    if ((owner as Group | Account | undefined)?._type === "Account") {
      throw new Error("Profiles should be owned by a group");
    }

    return super.create<M>(init, options);
  }
}

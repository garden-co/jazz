import { CoID } from "cojson";
import { CoValueClass, co } from "../internal.js";
import { Account } from "./account.js";
import { CoMap, CoMapInit, Simplify } from "./coMap.js";
import { Group } from "./group.js";
import { InboxInvite, InboxRoot } from "./inbox.js";

/** @category Identity & Permissions */
export class Profile extends CoMap {
  name = co.string;
  inbox = co.optional.json<CoID<InboxRoot>>();
  inboxInvite = co.optional.json<InboxInvite>();

  override get _owner(): Group {
    return super._owner as Group;
  }

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

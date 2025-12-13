import {
  Account,
  CoMap,
  CoValueClass,
  Group,
  Simplify,
  TypeSym,
  coField,
} from "../internal.js";

/** @category Identity & Permissions */
export class Profile extends CoMap {
  static fields = {
    name: "json" as const,
    inbox: "json" as const,
    inboxInvite: "json" as const,
  };

  declare readonly name: string;
  declare readonly inbox: string | undefined;
  declare readonly inboxInvite: string | undefined;

  /**
   * Creates a new profile with the given initial values and owner.
   *
   * The owner (a Group) determines access rights to the Profile.
   *
   * @category Creation
   * @deprecated Use `co.profile(...).create` instead.
   */
  static override create<M extends CoMap>(
    this: CoValueClass<M>,
    init: object,
    options?:
      | {
          owner: Group;
        }
      | Group,
  ) {
    const owner =
      options !== undefined && "owner" in options ? options.owner : options;

    // We add some guardrails to ensure that the owner of a profile is a group
    if ((owner as Group | Account | undefined)?.[TypeSym] === "Account") {
      throw new Error("Profiles should be owned by a group");
    }

    return super.create<M>(init, options);
  }
}

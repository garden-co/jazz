import {
  Account,
  CoMap,
  CoMapInit_DEPRECATED,
  CoValueClass,
  Group,
  Simplify,
  TypeSym,
} from "../internal.js";

/** @category Identity & Permissions */
export class Profile extends CoMap {
  declare readonly name: string;
  declare readonly inbox?: string;
  declare readonly inboxInvite?: string;

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
    init: Simplify<CoMapInit_DEPRECATED<M>>,
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

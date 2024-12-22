import { base58 } from "@scure/base";
import { CoID } from "../coValue.js";
import { CoValueUniqueness } from "../coValueCore.js";
import { Encrypted, KeyID, KeySecret, Sealed } from "../crypto/crypto.js";
import {
  AgentID,
  ChildGroupReference,
  ParentGroupReference,
  getChildGroupId,
  getParentGroupId,
  isAgentID,
  isChildGroupReference,
  isParentGroupReference,
} from "../ids.js";
import { JsonObject } from "../jsonValue.js";
import { Role } from "../permissions.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import {
  ControlledAccountOrAgent,
  RawAccount,
  RawAccountID,
} from "./account.js";
import { RawCoList } from "./coList.js";
import { RawCoMap } from "./coMap.js";
import { RawBinaryCoStream, RawCoStream } from "./coStream.js";

export const EVERYONE = "everyone" as const;
export type Everyone = "everyone";

export type GroupShape = {
  profile: CoID<RawCoMap> | null;
  root: CoID<RawCoMap> | null;
  [key: RawAccountID | AgentID]: Role;
  [EVERYONE]?: Role;
  readKey?: KeyID;
  [revelationFor: `${KeyID}_for_${RawAccountID | AgentID}`]: Sealed<KeySecret>;
  [revelationFor: `${KeyID}_for_${Everyone}`]: KeySecret;
  [oldKeyForNewKey: `${KeyID}_for_${KeyID}`]: Encrypted<
    KeySecret,
    { encryptedID: KeyID; encryptingID: KeyID }
  >;
  [parent: ParentGroupReference]: "extend";
  [child: ChildGroupReference]: "extend";
};

/** A `Group` is a scope for permissions of its members (`"reader" | "writer" | "admin"`), applying to objects owned by that group.
 *
 *  A `Group` object exposes methods for permission management and allows you to create new CoValues owned by that group.
 *
 *  (Internally, a `Group` is also just a `CoMap`, mapping member accounts to roles and containing some
 *  state management for making cryptographic keys available to current members)
 *
 *  @example
 *  You typically get a group from a CoValue that you already have loaded:
 *
 *  ```typescript
 *  const group = coMap.group;
 *  ```
 *
 *  @example
 *  Or, you can create a new group with a `LocalNode`:
 *
 *  ```typescript
 *  const localNode.createGroup();
 *  ```
 * */
export class RawGroup<
  Meta extends JsonObject | null = JsonObject | null,
> extends RawCoMap<GroupShape, Meta> {
  /**
   * Returns the current role of a given account.
   *
   * @category 1. Role reading
   */
  roleOf(accountID: RawAccountID): Role | undefined {
    return this.roleOfInternal(accountID)?.role;
  }

  /** @internal */
  roleOfInternal(
    accountID: RawAccountID | AgentID | typeof EVERYONE,
  ): { role: Role; via: CoID<RawGroup> | undefined } | undefined {
    const roleHere = this.get(accountID);

    if (roleHere === "revoked") {
      return undefined;
    }

    let roleInfo:
      | {
          role: Exclude<Role, "revoked">;
          via: CoID<RawGroup> | undefined;
        }
      | undefined = roleHere && { role: roleHere, via: undefined };

    const parentGroups = this.getParentGroups(this.options?.atTime);

    for (const parentGroup of parentGroups) {
      const roleInParent = parentGroup.roleOfInternal(accountID);

      if (
        roleInParent &&
        roleInParent.role !== "revoked" &&
        isMorePermissiveAndShouldInherit(roleInParent.role, roleInfo?.role)
      ) {
        roleInfo = { role: roleInParent.role, via: parentGroup.id };
      }
    }

    return roleInfo;
  }

  getParentGroups(atTime?: number) {
    const groups: RawGroup[] = [];

    for (const key of this.keys()) {
      if (isParentGroupReference(key)) {
        const parent = this.core.node.expectCoValueLoaded(
          getParentGroupId(key),
          "Expected parent group to be loaded",
        );

        const parentGroup = expectGroup(parent.getCurrentContent());

        if (atTime) {
          groups.push(parentGroup.atTime(atTime));
        } else {
          groups.push(parentGroup);
        }
      }
    }

    return groups;
  }

  loadAllChildGroups() {
    const requests: Promise<unknown>[] = [];
    const store = this.core.node.coValuesStore;

    for (const key of this.keys()) {
      if (!isChildGroupReference(key)) {
        continue;
      }

      const id = getChildGroupId(key);
      const child = store.get(id);

      // NOTE the same code invoked form another end (vs entry.load...)
      this.core.node.load(id).catch(() => {
        console.error(`Failed to load child group ${id}`);
      });

      requests.push(
        child.getCoValue().then((coValue) => {
          if (coValue === "unavailable") {
            throw new Error(`Child group ${child.id} is unavailable`);
          }

          // Recursively load child groups
          return expectGroup(coValue.getCurrentContent()).loadAllChildGroups();
        }),
      );
    }

    return Promise.all(requests);
  }

  getChildGroups() {
    const groups: RawGroup[] = [];

    for (const key of this.keys()) {
      if (isChildGroupReference(key)) {
        const child = this.core.node.expectCoValueLoaded(
          getChildGroupId(key),
          "Expected child group to be loaded",
        );
        groups.push(expectGroup(child.getCurrentContent()));
      }
    }

    return groups;
  }

  /**
   * Returns the role of the current account in the group.
   *
   * @category 1. Role reading
   */
  myRole(): Role | undefined {
    return this.roleOfInternal(this.core.node.account.id)?.role;
  }

  /**
   * Directly grants a new member a role in the group. The current account must be an
   * admin to be able to do so. Throws otherwise.
   *
   * @category 2. Role changing
   */
  addMember(
    account: RawAccount | ControlledAccountOrAgent | Everyone,
    role: Role,
  ) {
    this.addMemberInternal(account, role);
  }

  /** @internal */
  addMemberInternal(
    account: RawAccount | ControlledAccountOrAgent | AgentID | Everyone,
    role: Role,
  ) {
    const currentReadKey = this.core.getCurrentReadKey();

    if (!currentReadKey.secret) {
      throw new Error("Can't add member without read key secret");
    }

    if (account === EVERYONE) {
      if (!(role === "reader" || role === "writer")) {
        throw new Error(
          "Can't make everyone something other than reader or writer",
        );
      }
      this.set(account, role, "trusting");

      if (this.get(account) !== role) {
        throw new Error("Failed to set role");
      }

      this.set(
        `${currentReadKey.id}_for_${EVERYONE}`,
        currentReadKey.secret,
        "trusting",
      );
    } else {
      const memberKey = typeof account === "string" ? account : account.id;
      const agent =
        typeof account === "string"
          ? account
          : account.currentAgentID()._unsafeUnwrap({ withStackTrace: true });
      this.set(memberKey, role, "trusting");

      if (this.get(memberKey) !== role) {
        throw new Error("Failed to set role");
      }

      this.set(
        `${currentReadKey.id}_for_${memberKey}`,
        this.core.crypto.seal({
          message: currentReadKey.secret,
          from: this.core.node.account.currentSealerSecret(),
          to: this.core.crypto.getAgentSealerID(agent),
          nOnceMaterial: {
            in: this.id,
            tx: this.core.nextTransactionID(),
          },
        }),
        "trusting",
      );
    }
  }

  /** @internal */
  rotateReadKey() {
    const currentlyPermittedReaders = this.keys().filter((key) => {
      if (key.startsWith("co_") || isAgentID(key)) {
        const role = this.get(key);
        return role === "admin" || role === "writer" || role === "reader";
      } else {
        return false;
      }
    }) as (RawAccountID | AgentID)[];

    // Get these early, so we fail fast if they are unavailable
    const parentGroups = this.getParentGroups();
    const childGroups = this.getChildGroups();

    const maybeCurrentReadKey = this.core.getCurrentReadKey();

    if (!maybeCurrentReadKey.secret) {
      throw new Error("Can't rotate read key secret we don't have access to");
    }

    const currentReadKey = {
      id: maybeCurrentReadKey.id,
      secret: maybeCurrentReadKey.secret,
    };

    const newReadKey = this.core.crypto.newRandomKeySecret();

    for (const readerID of currentlyPermittedReaders) {
      const reader = this.core.node
        .resolveAccountAgent(
          readerID,
          "Expected to know currently permitted reader",
        )
        ._unsafeUnwrap({ withStackTrace: true });

      this.set(
        `${newReadKey.id}_for_${readerID}`,
        this.core.crypto.seal({
          message: newReadKey.secret,
          from: this.core.node.account.currentSealerSecret(),
          to: this.core.crypto.getAgentSealerID(reader),
          nOnceMaterial: {
            in: this.id,
            tx: this.core.nextTransactionID(),
          },
        }),
        "trusting",
      );
    }

    this.set(
      `${currentReadKey.id}_for_${newReadKey.id}`,
      this.core.crypto.encryptKeySecret({
        encrypting: newReadKey,
        toEncrypt: currentReadKey,
      }).encrypted,
      "trusting",
    );

    this.set("readKey", newReadKey.id, "trusting");

    // when we rotate our readKey (because someone got kicked out), we also need to (recursively)
    // rotate the readKeys of all child groups (so they are kicked out there as well)
    for (const parent of parentGroups) {
      const { id: parentReadKeyID, secret: parentReadKeySecret } =
        parent.core.getCurrentReadKey();

      if (!parentReadKeySecret) {
        throw new Error(
          "Can't reveal new child key to parent where we don't have access to the parent read key",
        );
      }

      this.set(
        `${newReadKey.id}_for_${parentReadKeyID}`,
        this.core.crypto.encryptKeySecret({
          encrypting: {
            id: parentReadKeyID,
            secret: parentReadKeySecret,
          },
          toEncrypt: newReadKey,
        }).encrypted,
        "trusting",
      );
    }

    for (const child of childGroups) {
      child.rotateReadKey();
    }
  }

  extend(parent: RawGroup) {
    if (parent.myRole() !== "admin" || this.myRole() !== "admin") {
      throw new Error(
        "To extend a group, the current account must have admin role in both groups",
      );
    }

    this.set(`parent_${parent.id}`, "extend", "trusting");
    parent.set(`child_${this.id}`, "extend", "trusting");

    const { id: parentReadKeyID, secret: parentReadKeySecret } =
      parent.core.getCurrentReadKey();
    if (!parentReadKeySecret) {
      throw new Error("Can't extend group without parent read key secret");
    }

    const { id: childReadKeyID, secret: childReadKeySecret } =
      this.core.getCurrentReadKey();
    if (!childReadKeySecret) {
      throw new Error("Can't extend group without child read key secret");
    }

    this.set(
      `${childReadKeyID}_for_${parentReadKeyID}`,
      this.core.crypto.encryptKeySecret({
        encrypting: {
          id: parentReadKeyID,
          secret: parentReadKeySecret,
        },
        toEncrypt: {
          id: childReadKeyID,
          secret: childReadKeySecret,
        },
      }).encrypted,
      "trusting",
    );
  }

  /**
   * Strips the specified member of all roles (preventing future writes in
   *  the group and owned values) and rotates the read encryption key for that group
   * (preventing reads of new content in the group and owned values)
   *
   * @category 2. Role changing
   */
  async removeMember(
    account: RawAccount | ControlledAccountOrAgent | Everyone,
  ) {
    // Ensure all child groups are loaded before removing a member
    await this.loadAllChildGroups();

    this.removeMemberInternal(account);
  }

  /** @internal */
  removeMemberInternal(
    account: RawAccount | ControlledAccountOrAgent | AgentID | Everyone,
  ) {
    const memberKey = typeof account === "string" ? account : account.id;
    this.set(memberKey, "revoked", "trusting");
    this.rotateReadKey();
  }

  /**
   * Creates an invite for new members to indirectly join the group,
   * allowing them to grant themselves the specified role with the InviteSecret
   * (a string starting with "inviteSecret_") - use `LocalNode.acceptInvite()` for this purpose.
   *
   * @category 2. Role changing
   */
  createInvite(role: "reader" | "writer" | "admin"): InviteSecret {
    const secretSeed = this.core.crypto.newRandomSecretSeed();

    const inviteSecret = this.core.crypto.agentSecretFromSecretSeed(secretSeed);
    const inviteID = this.core.crypto.getAgentID(inviteSecret);

    this.addMemberInternal(inviteID, `${role}Invite` as Role);

    return inviteSecretFromSecretSeed(secretSeed);
  }

  /**
   * Creates a new `CoMap` within this group, with the specified specialized
   * `CoMap` type `M` and optional static metadata.
   *
   * @category 3. Value creation
   */
  createMap<M extends RawCoMap>(
    init?: M["_shape"],
    meta?: M["headerMeta"],
    initPrivacy: "trusting" | "private" = "private",
    uniqueness: CoValueUniqueness = this.core.crypto.createdNowUnique(),
  ): M {
    const map = this.core.node
      .createCoValue({
        type: "comap",
        ruleset: {
          type: "ownedByGroup",
          group: this.id,
        },
        meta: meta || null,
        ...uniqueness,
      })
      .getCurrentContent() as M;

    if (init) {
      for (const [key, value] of Object.entries(init)) {
        map.set(key, value, initPrivacy);
      }
    }

    return map;
  }

  /**
   * Creates a new `CoList` within this group, with the specified specialized
   * `CoList` type `L` and optional static metadata.
   *
   * @category 3. Value creation
   */
  createList<L extends RawCoList>(
    init?: L["_item"][],
    meta?: L["headerMeta"],
    initPrivacy: "trusting" | "private" = "private",
    uniqueness: CoValueUniqueness = this.core.crypto.createdNowUnique(),
  ): L {
    const list = this.core.node
      .createCoValue({
        type: "colist",
        ruleset: {
          type: "ownedByGroup",
          group: this.id,
        },
        meta: meta || null,
        ...uniqueness,
      })
      .getCurrentContent() as L;

    if (init) {
      for (const item of init) {
        list.append(item, undefined, initPrivacy);
      }
    }

    return list;
  }

  /** @category 3. Value creation */
  createStream<C extends RawCoStream>(
    meta?: C["headerMeta"],
    uniqueness: CoValueUniqueness = this.core.crypto.createdNowUnique(),
  ): C {
    return this.core.node
      .createCoValue({
        type: "costream",
        ruleset: {
          type: "ownedByGroup",
          group: this.id,
        },
        meta: meta || null,
        ...uniqueness,
      })
      .getCurrentContent() as C;
  }

  /** @category 3. Value creation */
  createBinaryStream<C extends RawBinaryCoStream>(
    meta: C["headerMeta"] = { type: "binary" },
    uniqueness: CoValueUniqueness = this.core.crypto.createdNowUnique(),
  ): C {
    return this.core.node
      .createCoValue({
        type: "costream",
        ruleset: {
          type: "ownedByGroup",
          group: this.id,
        },
        meta: meta,
        ...uniqueness,
      })
      .getCurrentContent() as C;
  }
}

function isMorePermissiveAndShouldInherit(
  roleInParent: Role,
  roleInChild: Exclude<Role, "revoked"> | undefined,
) {
  // invites should never be inherited
  if (
    roleInParent === "adminInvite" ||
    roleInParent === "writerInvite" ||
    roleInParent === "readerInvite"
  ) {
    return false;
  }

  if (roleInParent === "admin") {
    return !roleInChild || roleInChild !== "admin";
  }

  if (roleInParent === "writer") {
    return !roleInChild || roleInChild === "reader";
  }

  if (roleInParent === "reader") {
    return !roleInChild;
  }

  return false;
}

export type InviteSecret = `inviteSecret_z${string}`;

function inviteSecretFromSecretSeed(secretSeed: Uint8Array): InviteSecret {
  return `inviteSecret_z${base58.encode(secretSeed)}`;
}

export function secretSeedFromInviteSecret(inviteSecret: InviteSecret) {
  if (!inviteSecret.startsWith("inviteSecret_z")) {
    throw new Error("Invalid invite secret");
  }

  return base58.decode(inviteSecret.slice("inviteSecret_z".length));
}

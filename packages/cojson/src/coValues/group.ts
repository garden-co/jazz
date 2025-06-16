import { base58 } from "@scure/base";
import { CoID } from "../coValue.js";
import { AvailableCoValueCore } from "../coValueCore/coValueCore.js";
import { CoValueUniqueness } from "../coValueCore/verifiedState.js";
import {
  CryptoProvider,
  Encrypted,
  KeyID,
  KeySecret,
  Sealed,
} from "../crypto/crypto.js";
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
import { logger } from "../logger.js";
import { AccountRole, Role } from "../permissions.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import {
  ControlledAccountOrAgent,
  RawAccount,
  RawAccountID,
} from "./account.js";
import { RawCoList } from "./coList.js";
import { RawCoMap } from "./coMap.js";
import { RawCoPlainText } from "./coPlainText.js";
import { RawBinaryCoStream, RawCoStream } from "./coStream.js";

export const EVERYONE = "everyone" as const;
export type Everyone = "everyone";

export type ParentGroupReferenceRole =
  | "revoked"
  | "extend"
  | "reader"
  | "writer"
  | "admin";

export type GroupShape = {
  profile: CoID<RawCoMap> | null;
  root: CoID<RawCoMap> | null;
  [key: RawAccountID | AgentID]: Role;
  [EVERYONE]?: Role;
  readKey?: KeyID;
  [writeKeyFor: `writeKeyFor_${RawAccountID | AgentID}`]: KeyID;
  [revelationFor: `${KeyID}_for_${RawAccountID | AgentID}`]: Sealed<KeySecret>;
  [revelationFor: `${KeyID}_for_${Everyone}`]: KeySecret;
  [oldKeyForNewKey: `${KeyID}_for_${KeyID}`]: Encrypted<
    KeySecret,
    { encryptedID: KeyID; encryptingID: KeyID }
  >;
  [parent: ParentGroupReference]: ParentGroupReferenceRole;
  [child: ChildGroupReference]: "revoked" | "extend";
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
  protected readonly crypto: CryptoProvider;

  constructor(
    core: AvailableCoValueCore,
    options?: {
      ignorePrivateTransactions: boolean;
    },
  ) {
    super(core, options);
    this.crypto = core.node.crypto;
  }

  /**
   * Returns the current role of a given account.
   *
   * @category 1. Role reading
   */
  roleOf(accountID: RawAccountID | typeof EVERYONE): Role | undefined {
    return this.roleOfInternal(accountID);
  }

  /**
   *  This is a performance-critical function, micro-optimizing it is important
   *
   *  Avoid to add objects/array allocations in this function
   */
  /** @internal */
  roleOfInternal(
    accountID: RawAccountID | AgentID | typeof EVERYONE,
  ): Role | undefined {
    let roleHere = this.get(accountID);

    if (roleHere === "revoked") {
      roleHere = undefined;
    }

    let roleInfo: Role | undefined = roleHere;

    for (const key of Object.keys(this.ops)) {
      if (!isParentGroupReference(key)) continue;

      const group = this.getParentGroupFromKey(key, this.atTimeFilter);

      if (!group) continue;

      const role = this.get(key) ?? "extend";
      const parentRole = group.roleOfInternal(accountID);

      if (!isInheritableRole(parentRole)) {
        continue;
      }

      const roleToInherit = role !== "extend" ? role : parentRole;

      if (isMorePermissiveAndShouldInherit(roleToInherit, roleInfo)) {
        roleInfo = roleToInherit;
      }
    }

    if (!roleInfo && accountID !== "everyone") {
      const everyoneRole = this.get("everyone");

      if (everyoneRole && everyoneRole !== "revoked") return everyoneRole;
    }

    return roleInfo;
  }

  getParentGroupFromKey(key: ParentGroupReference, atTime?: number) {
    if (this.get(key) === "revoked") {
      return null;
    }

    const parent = this.core.node.expectCoValueLoaded(
      getParentGroupId(key),
      "Expected parent group to be loaded",
    );

    const group = expectGroup(parent.getCurrentContent());

    if (atTime) {
      return group.atTime(atTime);
    } else {
      return group;
    }
  }

  getParentGroups(atTime?: number) {
    const groups: RawGroup[] = [];

    for (const key of Object.keys(this.ops)) {
      if (!isParentGroupReference(key)) continue;

      const group = this.getParentGroupFromKey(key, atTime);

      if (group) {
        if (atTime) {
          groups.push(group.atTime(atTime));
        } else {
          groups.push(group);
        }
      }
    }

    return groups;
  }

  loadAllChildGroups() {
    const requests: Promise<unknown>[] = [];
    const peers = this.core.node.syncManager.getServerAndStoragePeers();

    for (const key of this.keys()) {
      if (!isChildGroupReference(key)) {
        continue;
      }

      const id = getChildGroupId(key);
      const child = this.core.node.getCoValue(id);

      if (
        child.loadingState === "unknown" ||
        child.loadingState === "unavailable"
      ) {
        child.loadFromPeers(peers).catch(() => {
          logger.error(`Failed to load child group ${id}`);
        });
      }

      requests.push(
        child.waitForAvailableOrUnavailable().then((coValue) => {
          if (!coValue.isAvailable()) {
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
        // Check if the child group reference is revoked
        if (this.get(key) === "revoked") {
          continue;
        }

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
    return this.roleOfInternal(this.core.node.getCurrentAgent().id);
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
    if (account === EVERYONE) {
      if (!(role === "reader" || role === "writer" || role === "writeOnly")) {
        throw new Error(
          "Can't make everyone something other than reader, writer or writeOnly",
        );
      }
      const currentReadKey = this.core.getCurrentReadKey();

      if (!currentReadKey.secret) {
        throw new Error("Can't add member without read key secret");
      }

      const previousRole = this.get(account);

      this.set(account, role, "trusting");

      if (this.get(account) !== role) {
        // The role was not set correctly; this presents three scenarios:
        // 1. The current user is an administrator trying to set another administrator to a lower role
        // 2. The current user is an administrator but something has gone wrong with the role assignment
        // 3. The current user is not an administrator and does not have sufficient permissions to set the role
        const myRole = this.myRole();
        throw new Error(
          myRole === "admin"
            ? this.get(account) === "admin"
              ? "Administrators cannot demote other administrators in a group"
              : "Failed to set role"
            : `Failed to set role due to insufficient permissions (role of current account is ${myRole})`,
        );
      }

      if (role === "writeOnly") {
        if (previousRole === "reader" || previousRole === "writer") {
          this.rotateReadKey();
        }

        this.delete(`${currentReadKey.id}_for_${EVERYONE}`);
      } else {
        this.set(
          `${currentReadKey.id}_for_${EVERYONE}`,
          currentReadKey.secret,
          "trusting",
        );
      }

      return;
    }

    const memberKey = typeof account === "string" ? account : account.id;
    const agent =
      typeof account === "string" ? account : account.currentAgentID();

    /**
     * WriteOnly members can only see their own changes.
     *
     * We don't want to reveal the readKey to them so we create a new one specifically for them and also reveal it to everyone else with a reader or higher-capability role (but crucially not to other writer-only members)
     * to everyone else.
     *
     * To never reveal the readKey to writeOnly members we also create a dedicated writeKey for the
     * invite.
     */
    if (role === "writeOnly" || role === "writeOnlyInvite") {
      const previousRole = this.get(memberKey);

      this.set(memberKey, role, "trusting");

      if (
        previousRole === "reader" ||
        previousRole === "writer" ||
        previousRole === "admin"
      ) {
        this.rotateReadKey();
      }

      this.internalCreateWriteOnlyKeyForMember(memberKey, agent);
    } else {
      const currentReadKey = this.core.getCurrentReadKey();

      if (!currentReadKey.secret) {
        throw new Error("Can't add member without read key secret");
      }

      this.set(memberKey, role, "trusting");

      if (this.get(memberKey) !== role) {
        const myRole = this.myRole();
        throw new Error(
          myRole === "admin"
            ? this.get(memberKey) === "admin"
              ? "Administrators cannot demote other administrators in a group"
              : "Failed to set role"
            : `Failed to set role due to insufficient permissions (role of current account is ${myRole})`,
        );
      }

      this.storeKeyRevelationForMember(
        memberKey,
        agent,
        currentReadKey.id,
        currentReadKey.secret,
      );

      for (const keyID of this.getWriteOnlyKeys()) {
        const secret = this.core.getReadKey(keyID);

        if (!secret) {
          logger.error("Can't find key " + keyID);
          continue;
        }

        this.storeKeyRevelationForMember(memberKey, agent, keyID, secret);
      }
    }
  }

  internalCreateWriteOnlyKeyForMember(
    memberKey: RawAccountID | AgentID,
    agent: AgentID,
  ) {
    const writeKeyForNewMember = this.crypto.newRandomKeySecret();

    this.set(`writeKeyFor_${memberKey}`, writeKeyForNewMember.id, "trusting");

    this.storeKeyRevelationForMember(
      memberKey,
      agent,
      writeKeyForNewMember.id,
      writeKeyForNewMember.secret,
    );

    for (const otherMemberKey of this.getMemberKeys()) {
      const memberRole = this.get(otherMemberKey);

      if (
        memberRole === "reader" ||
        memberRole === "writer" ||
        memberRole === "admin" ||
        memberRole === "readerInvite" ||
        memberRole === "writerInvite" ||
        memberRole === "adminInvite"
      ) {
        const otherMemberAgent = this.core.node
          .resolveAccountAgent(
            otherMemberKey,
            "Expected member agent to be loaded",
          )
          ._unsafeUnwrap({ withStackTrace: true });

        this.storeKeyRevelationForMember(
          otherMemberKey,
          otherMemberAgent,
          writeKeyForNewMember.id,
          writeKeyForNewMember.secret,
        );
      }
    }
  }

  private storeKeyRevelationForMember(
    memberKey: RawAccountID | AgentID,
    agent: AgentID,
    keyID: KeyID,
    secret: KeySecret,
  ) {
    this.set(
      `${keyID}_for_${memberKey}`,
      this.crypto.seal({
        message: secret,
        from: this.core.node.getCurrentAgent().currentSealerSecret(),
        to: this.crypto.getAgentSealerID(agent),
        nOnceMaterial: {
          in: this.id,
          tx: this.core.nextTransactionID(),
        },
      }),
      "trusting",
    );
  }

  private getWriteOnlyKeys() {
    const keys: KeyID[] = [];

    for (const key of this.keys()) {
      if (key.startsWith("writeKeyFor_")) {
        keys.push(
          this.get(key as `writeKeyFor_${RawAccountID | AgentID}`) as KeyID,
        );
      }
    }

    return keys;
  }

  getCurrentReadKeyId() {
    const myRole = this.myRole();

    if (myRole === "writeOnly") {
      const accountId = this.core.node.getCurrentAgent().id;

      const key = this.get(`writeKeyFor_${accountId}`) as KeyID;

      // When everyone is writeOnly, we need to create a writeOnly key for the current account if missing
      if (!key && this.get("everyone") === "writeOnly") {
        this.internalCreateWriteOnlyKeyForMember(
          accountId,
          this.core.node.getCurrentAgent().currentAgentID(),
        );

        return this.get(`writeKeyFor_${accountId}`) as KeyID;
      }

      return key;
    }

    if (!myRole) {
      const accountId = this.core.node.getCurrentAgent().id;

      const key = this.get(`writeKeyFor_${accountId}`) as KeyID;

      if (key) {
        return key;
      }
    }

    return this.get("readKey");
  }

  getMemberKeys(): (RawAccountID | AgentID)[] {
    return this.keys().filter((key): key is RawAccountID | AgentID => {
      return key.startsWith("co_") || isAgentID(key);
    });
  }

  getAllMemberKeysSet() {
    const memberKeys = new Set(this.getMemberKeys());

    for (const group of this.getParentGroups()) {
      for (const key of group.getAllMemberKeysSet()) {
        memberKeys.add(key);
      }
    }

    return memberKeys;
  }

  /** @internal */
  rotateReadKey(removedMemberKey?: RawAccountID | AgentID | "everyone") {
    const memberKeys = this.getMemberKeys().filter(
      (key) => key !== removedMemberKey,
    );

    const currentlyPermittedReaders = memberKeys.filter((key) => {
      const role = this.get(key);
      return (
        role === "admin" ||
        role === "writer" ||
        role === "reader" ||
        role === "adminInvite" ||
        role === "writerInvite" ||
        role === "readerInvite"
      );
    });

    const writeOnlyMembers = memberKeys.filter((key) => {
      const role = this.get(key);
      return role === "writeOnly" || role === "writeOnlyInvite";
    });

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

    const newReadKey = this.crypto.newRandomKeySecret();

    for (const readerID of currentlyPermittedReaders) {
      const agent = this.core.node
        .resolveAccountAgent(
          readerID,
          "Expected to know currently permitted reader",
        )
        ._unsafeUnwrap({ withStackTrace: true });

      this.storeKeyRevelationForMember(
        readerID,
        agent,
        newReadKey.id,
        newReadKey.secret,
      );
    }

    /**
     * If there are some writeOnly members we need to rotate their keys
     * and reveal them to the other non-writeOnly members
     */
    for (const writeOnlyMemberID of writeOnlyMembers) {
      const agent = this.core.node
        .resolveAccountAgent(
          writeOnlyMemberID,
          "Expected to know writeOnly member",
        )
        ._unsafeUnwrap({ withStackTrace: true });

      const writeOnlyKey = this.crypto.newRandomKeySecret();

      this.storeKeyRevelationForMember(
        writeOnlyMemberID,
        agent,
        writeOnlyKey.id,
        writeOnlyKey.secret,
      );
      this.set(`writeKeyFor_${writeOnlyMemberID}`, writeOnlyKey.id, "trusting");

      for (const readerID of currentlyPermittedReaders) {
        const agent = this.core.node
          .resolveAccountAgent(
            readerID,
            "Expected to know currently permitted reader",
          )
          ._unsafeUnwrap({ withStackTrace: true });

        this.storeKeyRevelationForMember(
          readerID,
          agent,
          writeOnlyKey.id,
          writeOnlyKey.secret,
        );
      }
    }

    this.set(
      `${currentReadKey.id}_for_${newReadKey.id}`,
      this.crypto.encryptKeySecret({
        encrypting: newReadKey,
        toEncrypt: currentReadKey,
      }).encrypted,
      "trusting",
    );

    this.set("readKey", newReadKey.id, "trusting");

    /**
     * The new read key needs to be revealed to the parent groups
     *
     * This way the members from the parent groups can still have access to this group
     */
    for (const parent of parentGroups) {
      const { id: parentReadKeyID, secret: parentReadKeySecret } =
        parent.core.getCurrentReadKey();

      if (!parentReadKeySecret) {
        // We can't reveal the new child key to the parent group where we don't have access to the parent read key
        // TODO: This will be fixed with: https://github.com/garden-co/jazz/issues/1979
        logger.warn(
          "Can't reveal new child key to parent where we don't have access to the parent read key",
        );
        continue;
      }

      this.set(
        `${newReadKey.id}_for_${parentReadKeyID}`,
        this.crypto.encryptKeySecret({
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
      // Since child references are mantained only for the key rotation,
      // circular references are skipped here because it's more performant
      // than always checking for circular references in childs inside the permission checks
      if (child.isSelfExtension(this)) {
        continue;
      }

      child.rotateReadKey(removedMemberKey);
    }
  }

  /** Detect circular references in group inheritance */
  isSelfExtension(parent: RawGroup) {
    if (parent.id === this.id) {
      return true;
    }

    const childGroups = this.getChildGroups();

    for (const child of childGroups) {
      if (child.isSelfExtension(parent)) {
        return true;
      }
    }

    return false;
  }

  extend(
    parent: RawGroup,
    role: "reader" | "writer" | "admin" | "inherit" = "inherit",
  ) {
    if (this.isSelfExtension(parent)) {
      return;
    }

    if (this.myRole() !== "admin") {
      throw new Error(
        "To extend a group, the current account must be an admin in the child group",
      );
    }

    const value = role === "inherit" ? "extend" : role;

    this.set(`parent_${parent.id}`, value, "trusting");
    parent.set(`child_${this.id}`, "extend", "trusting");

    if (
      parent.myRole() !== "admin" &&
      parent.myRole() !== "writer" &&
      parent.myRole() !== "reader" &&
      parent.myRole() !== "writeOnly"
    ) {
      // Create a writeOnly key in the parent group to be able to reveal the current child key to the parent group
      parent.internalCreateWriteOnlyKeyForMember(
        this.core.node.getCurrentAgent().id,
        this.core.node.getCurrentAgent().currentAgentID(),
      );
    }

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
      this.crypto.encryptKeySecret({
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

  async revokeExtend(parent: RawGroup) {
    if (this.myRole() !== "admin") {
      throw new Error(
        "To unextend a group, the current account must be an admin in the child group",
      );
    }

    if (
      parent.myRole() !== "admin" &&
      parent.myRole() !== "writer" &&
      parent.myRole() !== "reader" &&
      parent.myRole() !== "writeOnly"
    ) {
      throw new Error(
        "To unextend a group, the current account must be a member of the parent group",
      );
    }

    if (
      !this.get(`parent_${parent.id}`) ||
      this.get(`parent_${parent.id}`) === "revoked"
    ) {
      return;
    }

    // Set the parent key on the child group to `revoked`
    this.set(`parent_${parent.id}`, "revoked", "trusting");

    // Set the child key on the parent group to `revoked`
    parent.set(`child_${this.id}`, "revoked", "trusting");

    await this.loadAllChildGroups();

    // Rotate the keys on the child group
    this.rotateReadKey();
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

    if (this.myRole() === "admin") {
      this.rotateReadKey(memberKey);
    }

    this.set(memberKey, "revoked", "trusting");
  }

  /**
   * Creates an invite for new members to indirectly join the group,
   * allowing them to grant themselves the specified role with the InviteSecret
   * (a string starting with "inviteSecret_") - use `LocalNode.acceptInvite()` for this purpose.
   *
   * @category 2. Role changing
   */
  createInvite(role: AccountRole): InviteSecret {
    const secretSeed = this.crypto.newRandomSecretSeed();

    const inviteSecret = this.crypto.agentSecretFromSecretSeed(secretSeed);
    const inviteID = this.crypto.getAgentID(inviteSecret);

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
    uniqueness: CoValueUniqueness = this.crypto.createdNowUnique(),
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
      map.assign(init, initPrivacy);
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
    uniqueness: CoValueUniqueness = this.crypto.createdNowUnique(),
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

    if (init?.length) {
      list.appendItems(init, undefined, initPrivacy);
    }

    return list;
  }

  /**
   * Creates a new `CoPlainText` within this group, with the specified specialized
   * `CoPlainText` type `T` and optional static metadata.
   *
   * @category 3. Value creation
   */
  createPlainText<T extends RawCoPlainText>(
    init?: string,
    meta?: T["headerMeta"],
    initPrivacy: "trusting" | "private" = "private",
  ): T {
    const text = this.core.node
      .createCoValue({
        type: "coplaintext",
        ruleset: {
          type: "ownedByGroup",
          group: this.id,
        },
        meta: meta || null,
        ...this.crypto.createdNowUnique(),
      })
      .getCurrentContent() as T;

    if (init) {
      text.insertAfter(0, init, initPrivacy);
    }

    return text;
  }

  /** @category 3. Value creation */
  createStream<C extends RawCoStream>(
    meta?: C["headerMeta"],
    uniqueness: CoValueUniqueness = this.crypto.createdNowUnique(),
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
    uniqueness: CoValueUniqueness = this.crypto.createdNowUnique(),
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

export function isInheritableRole(
  roleInParent: Role | undefined,
): roleInParent is "revoked" | "admin" | "writer" | "reader" {
  return (
    roleInParent === "revoked" ||
    roleInParent === "admin" ||
    roleInParent === "writer" ||
    roleInParent === "reader"
  );
}

function isMorePermissiveAndShouldInherit(
  roleInParent: "revoked" | "admin" | "writer" | "reader",
  roleInChild: Role | undefined,
) {
  if (roleInParent === "revoked") {
    return true;
  }

  if (roleInParent === "admin") {
    return !roleInChild || roleInChild !== "admin";
  }

  if (roleInParent === "writer") {
    return (
      !roleInChild || roleInChild === "reader" || roleInChild === "writeOnly"
    );
  }

  if (roleInParent === "reader") {
    return !roleInChild;
  }

  // writeOnly can't be inherited
  if (roleInParent === "writeOnly") {
    return false;
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

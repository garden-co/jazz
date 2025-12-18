import {
  RawAccount,
  isAccountRole,
  type AccountRole,
  type AgentID,
  type Everyone,
  type InviteSecret,
  type RawAccountID,
  type RawGroup,
  type Role,
} from "cojson";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  ID,
  Settled,
  RefEncoded,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  CoMapFieldSchema,
  Profile,
  MaybeLoaded,
  GroupSchema,
  coGroupDefiner,
  coProfileDefiner,
  coAccountDefiner,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  CoProfileSchema,
  CoreAccountSchema,
  CoreCoValueSchema,
} from "../internal.js";
import {
  Account,
  AccountAndGroupProxyHandler,
  CoValueBase,
  CoValueJazzApi,
  Ref,
  RegisteredSchemas,
  accessChildById,
  activeAccountContext,
  ensureCoValueLoaded,
  isControlledAccount,
  loadCoValueWithoutMe,
  parseGroupCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
  CoreGroupSchema,
} from "../internal.js";

type GroupMember = {
  id: string;
  role: AccountRole;
  ref: Ref<CoreAccountSchema>;
  account: MaybeLoaded<CoreAccountSchema>;
};

/**
 * Roles that can be granted to a group member.
 */
export type GroupRole = "reader" | "writer" | "admin" | "manager";

/** @category Identity & Permissions */
export class Group extends CoValueBase implements CoValue {
  declare [TypeSym]: "Group";
  static {
    this.prototype[TypeSym] = "Group";
  }
  declare $jazz: GroupJazzApi<this>;

  static fields = {
    profile: {
      type: "ref",
      optional: true,
      sourceSchema: coProfileDefiner(),
    } satisfies RefEncoded<CoProfileSchema>,
  };

  declare readonly profile: MaybeLoaded<CoProfileSchema> | undefined;

  /** @deprecated Don't use constructor directly, use .create */
  constructor(raw: RawGroup, sourceSchema: CoreGroupSchema) {
    super();

    if (!raw) {
      throw new Error("Raw group is required");
    }

    const proxy = new Proxy(
      this,
      AccountAndGroupProxyHandler as ProxyHandler<this>,
    );

    Object.defineProperties(this, {
      $jazz: {
        value: new GroupJazzApi(proxy, raw, sourceSchema),
        enumerable: false,
      },
    });

    return proxy as this;
  }

  myRole(): Role | undefined {
    return this.$jazz.raw.myRole();
  }

  addMember(member: Everyone, role: "writer" | "reader" | "writeOnly"): void;
  addMember(member: Loaded<CoreAccountSchema, true>, role: AccountRole): void;
  /** @category Identity & Permissions
   * Gives members of a parent group membership in this group.
   * @param member The group that will gain access to this group.
   * @param role The role all members of the parent group should have in this group.
   */
  addMember(member: Group, role?: GroupRole | "inherit"): void;
  addMember(
    member: Loaded<CoreGroupSchema> | Loaded<CoreAccountSchema, true>,
    role: "reader" | "writer" | "admin" | "manager",
  ): void;
  addMember(
    member:
      | Loaded<CoreGroupSchema>
      | Loaded<CoreAccountSchema, true>
      | Everyone,
    role?: AccountRole | "inherit",
  ): void {
    if (isGroupValue(member)) {
      if (role === "writeOnly")
        throw new Error("Cannot add group as member with write-only role");
      this.$jazz.raw.extend(member.$jazz.raw, role);
    } else if (role !== undefined && role !== "inherit") {
      this.$jazz.raw.addMember(
        member === "everyone" ? member : member.$jazz.raw,
        role,
      );
    }
  }

  removeMember(member: Everyone | Loaded<CoreAccountSchema, true>): void;
  /** @category Identity & Permissions
   * Revokes membership from members a parent group.
   * @param member The group that will lose access to this group.
   */
  removeMember(member: Loaded<CoreGroupSchema>): void;
  removeMember(
    member:
      | Loaded<CoreGroupSchema>
      | Loaded<CoreAccountSchema, true>
      | Everyone,
  ): void;
  removeMember(
    member:
      | Loaded<CoreGroupSchema>
      | Loaded<CoreAccountSchema, true>
      | Everyone,
  ): void {
    if (isGroupValue(member)) {
      this.$jazz.raw.revokeExtend(member.$jazz.raw);
    } else {
      return this.$jazz.raw.removeMember(
        member === "everyone" ? member : member.$jazz.raw,
      );
    }
  }

  private getMembersFromKeys(
    accountIDs: Iterable<RawAccountID | AgentID>,
  ): GroupMember[] {
    const members = [];

    const refEncodedAccountSchema = {
      type: "ref",
      optional: false,
      sourceSchema: coAccountDefiner(),
    } satisfies RefEncoded<CoreAccountSchema>;

    for (const accountID of accountIDs) {
      if (!isAccountID(accountID)) continue;

      const role = this.$jazz.raw.roleOf(accountID);

      if (isAccountRole(role)) {
        const ref = new Ref<CoreAccountSchema>(
          accountID,
          this.$jazz.loadedAs,
          refEncodedAccountSchema,
          this,
        );

        const group = this;

        members.push({
          id: accountID as unknown as ID<Loaded<CoreAccountSchema, true>>,
          role,
          ref,
          get account() {
            // Accounts values are non-nullable because are loaded as dependencies
            return accessChildById(
              group,
              accountID,
              refEncodedAccountSchema,
            ) as MaybeLoaded<CoreAccountSchema>;
          },
        });
      }
    }

    return members;
  }

  /**
   * Returns all members of the group, including inherited members from parent
   * groups.
   *
   * If you need only the direct members of the group, use
   * {@link getDirectMembers} instead.
   *
   * @returns The members of the group.
   */
  get members(): GroupMember[] {
    return this.getMembersFromKeys(this.$jazz.raw.getAllMemberKeysSet());
  }

  /**
   * Returns the direct members of the group.
   *
   * If you need all members of the group, including inherited members from
   * parent groups, use {@link Group.members|members} instead.
   * @returns The direct members of the group.
   */
  getDirectMembers(): GroupMember[] {
    return this.getMembersFromKeys(this.$jazz.raw.getMemberKeys());
  }

  getRoleOf(
    member: Everyone | ID<Loaded<CoreAccountSchema, true>> | "me",
  ): Role | undefined {
    const accountId =
      member === "me"
        ? (activeAccountContext.get().$jazz.id as RawAccountID)
        : member === "everyone"
          ? member
          : (member as RawAccountID);
    return this.$jazz.raw.roleOf(accountId);
  }

  /**
   * Make the group public, so that everyone can read it.
   * Alias for `addMember("everyone", role)`.
   *
   * @param role - Optional: the role to grant to everyone. Defaults to "reader".
   * @returns The group itself.
   */
  makePublic(role: "reader" | "writer" = "reader"): this {
    this.addMember("everyone", role);
    return this;
  }

  getParentGroups(): Array<Group> {
    return this.$jazz.raw.getParentGroups().map((group) => {
      // Use the schema's fromRaw method
      const schema = this.$jazz.sourceSchema;
      if (schema && "fromRaw" in schema) {
        return (schema as any).fromRaw(group);
      }
      // Fallback for backward compatibility
      return new Group(group, schema!);
    });
  }

  /** @category Identity & Permissions
   * Gives members of a parent group membership in this group.
   * @deprecated Use `addMember` instead.
   * @param parent The group that will gain access to this group.
   * @param roleMapping The role all members of the parent group should have in this group.
   * @returns This group.
   */
  extend(
    parent: Group,
    roleMapping?: "reader" | "writer" | "admin" | "manager" | "inherit",
  ): this {
    this.$jazz.raw.extend(
      parent.$jazz.raw,
      roleMapping as "reader" | "writer" | "admin" | "manager" | "inherit",
    );
    return this;
  }

  /** @category Identity & Permissions
   * Revokes membership from members a parent group.
   * @deprecated Use `removeMember` instead.
   * @param parent The group that will lose access to this group.
   * @returns This group.
   */
  async revokeExtend(parent: Group): Promise<this> {
    await this.$jazz.raw.revokeExtend(parent.$jazz.raw);
    return this;
  }
}

export class GroupJazzApi<G extends Group> extends CoValueJazzApi<G> {
  constructor(
    private group: G,
    public raw: RawGroup,
    public sourceSchema: CoreGroupSchema,
  ) {
    super(group);

    if (!this.sourceSchema) {
      throw new Error("sourceSchema is required");
    }
  }

  /**
   * The ID of this `Group`
   * @category Content
   */
  get id(): ID<G> {
    return this.raw.id;
  }

  /**
   * Groups have no owner. They can be accessed by everyone.
   */
  get owner(): undefined {
    return undefined;
  }

  /** @category Subscription & Loading */
  ensureLoaded<const R extends ResolveQuery<CoreGroupSchema>>(
    this: GroupJazzApi<G>,
    options?: { resolve?: ResolveQueryStrict<CoreGroupSchema, R> },
  ): Promise<Loaded<CoreGroupSchema, R>> {
    return ensureCoValueLoaded<Group, CoreGroupSchema, R>(this.group, options);
  }

  /** @category Subscription & Loading */
  subscribe<const R extends ResolveQuery<CoreGroupSchema>>(
    this: GroupJazzApi<G>,
    listener: (
      value: Loaded<CoreGroupSchema, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<const R extends ResolveQuery<CoreGroupSchema>>(
    this: GroupJazzApi<G>,
    options: { resolve?: ResolveQueryStrict<CoreGroupSchema, R> },
    listener: (
      value: Loaded<CoreGroupSchema, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<const R extends ResolveQuery<CoreGroupSchema>>(
    this: GroupJazzApi<G>,
    ...args: SubscribeRestArgs<CoreGroupSchema, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this.group, options, listener);
  }

  /**
   * Create an invite to this group
   *
   * @category Invites
   */
  createInvite(role: AccountRole = "reader"): InviteSecret {
    return this.raw.createInvite(role);
  }

  /**
   * Wait for the `Group` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }
}

export function isAccountID(id: RawAccountID | AgentID): id is RawAccountID {
  return id.startsWith("co_");
}

export function getCoValueOwner(
  coValue: Loaded<CoreCoValueSchema>,
): Loaded<CoreGroupSchema> {
  const group = accessChildById(coValue, coValue.$jazz.raw.group.id, {
    type: "ref",
    sourceSchema: coGroupDefiner(),
    optional: false,
  });
  if (!group.$isLoaded) {
    throw new Error("CoValue has no owner");
  }
  return group;
}

function isGroupValue(
  value: Loaded<CoreGroupSchema> | Everyone | Loaded<CoreAccountSchema, true>,
): value is Loaded<CoreGroupSchema> {
  return value !== "everyone" && !(value.$jazz.raw instanceof RawAccount);
}

import {
  AgentSecret,
  CoID,
  ControlledAccount as RawControlledAccount,
  CryptoProvider,
  Everyone,
  InviteSecret,
  LocalNode,
  Peer,
  RawAccount,
  RawCoMap,
  RawCoValue,
  SessionID,
  cojsonInternals,
  isAccountRole,
} from "cojson";
import {
  AnonymousJazzAgent,
  BranchDefinition,
  CoFieldInit,
  type CoMap,
  type CoValue,
  CoValueBase,
  CoValueClass,
  CoValueClassOrSchema,
  CoValueJazzApi,
  Group,
  ID,
  InstanceOrPrimitiveOfSchema,
  Profile,
  Ref,
  type RefEncoded,
  RefIfCoValue,
  RefsToResolve,
  RefsToResolveStrict,
  RegisteredSchemas,
  Resolved,
  SchemaInit,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  accessChildByKey,
  accountOrGroupToGroup,
  activeAccountContext,
  coValueClassFromCoValueClassOrSchema,
  coValuesCache,
  createInboxRoot,
  ensureCoValueLoaded,
  inspect,
  instantiateRefEncodedWithInit,
  loadCoValue,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
} from "../internal.js";

export type AccountCreationProps = {
  name: string;
  onboarding?: boolean;
};

/** @category Identity & Permissions */
export class Account extends CoValueBase implements CoValue {
  declare [TypeSym]: "Account";

  /**
   * Jazz methods for Accounts are inside this property.
   *
   * This allows Accounts to be used as plain objects while still having
   * access to Jazz methods.
   */
  declare $jazz: AccountJazzApi<this>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any = {
    profile: {
      ref: () => Profile,
      optional: false,
    } satisfies RefEncoded<Profile>,
    root: {
      ref: () => RegisteredSchemas["CoMap"],
      optional: true,
    } satisfies RefEncoded<CoMap>,
  };

  declare readonly profile: Profile | null;
  declare readonly root: CoMap | null;

  constructor(options: { fromRaw: RawAccount }) {
    super();
    if (!("fromRaw" in options)) {
      throw new Error("Can only construct account from raw or with .create()");
    }

    const proxy = new Proxy(
      this,
      AccountAndGroupProxyHandler as ProxyHandler<this>,
    );

    Object.defineProperties(this, {
      [TypeSym]: { value: "Account", enumerable: false },
      $jazz: {
        value: new AccountJazzApi(proxy, options.fromRaw),
        enumerable: false,
      },
    });

    return proxy;
  }

  /**
   * Whether this account is the currently active account.
   */
  get isMe(): boolean {
    return activeAccountContext.get().$jazz.id === this.$jazz.id;
  }

  /**
   * Accept an invite to a `CoValue` or `Group`.
   *
   * @param valueID The ID of the `CoValue` or `Group` to accept the invite to.
   * @param inviteSecret The secret of the invite to accept.
   * @param coValueClass [Group] The class of the `CoValue` or `Group` to accept the invite to.
   * @returns The loaded `CoValue` or `Group`.
   */
  async acceptInvite<S extends CoValueClassOrSchema>(
    valueID: string,
    inviteSecret: InviteSecret,
    coValueClass?: S,
  ): Promise<Resolved<InstanceOrPrimitiveOfSchema<S>, true> | null> {
    if (!this.$jazz.isLocalNodeOwner) {
      throw new Error("Only a controlled account can accept invites");
    }

    await this.$jazz.localNode.acceptInvite(
      valueID as unknown as CoID<RawCoValue>,
      inviteSecret,
    );

    return loadCoValue(
      coValueClassFromCoValueClassOrSchema(coValueClass ?? Group),
      valueID,
      {
        loadAs: this,
      },
    ) as Resolved<InstanceOrPrimitiveOfSchema<S>, true> | null;
  }

  getRoleOf(member: Everyone | ID<Account> | "me"): "admin" | undefined {
    if (member === "me") {
      return this.isMe ? "admin" : undefined;
    }

    if (member === this.$jazz.id) {
      return "admin";
    }

    return undefined;
  }

  canRead(value: CoValue): boolean {
    const valueOwner = value.$jazz.owner;
    if (!valueOwner) {
      // Groups and Accounts are public
      return true;
    }
    const role = valueOwner.getRoleOf(this.$jazz.id);

    return isAccountRole(role);
  }

  canWrite(value: CoValue): boolean {
    const valueOwner = value.$jazz.owner;
    if (!valueOwner) {
      if (value[TypeSym] === "Group") {
        const roleInGroup = (value as Group).getRoleOf(this.$jazz.id);
        return (
          roleInGroup === "admin" ||
          roleInGroup === "manager" ||
          roleInGroup === "writer"
        );
      }
      if (value[TypeSym] === "Account") {
        return value.$jazz.id === this.$jazz.id;
      }
      return false;
    }
    const role = valueOwner.getRoleOf(this.$jazz.id);

    return (
      role === "admin" ||
      role === "manager" ||
      role === "writer" ||
      role === "writeOnly"
    );
  }

  canManage(value: CoValue): boolean {
    const valueOwner = value.$jazz.owner;
    if (!valueOwner) {
      if (value[TypeSym] === "Group") {
        const roleInGroup = (value as Group).getRoleOf(this.$jazz.id);
        return roleInGroup === "manager" || roleInGroup === "admin";
      }
      if (value[TypeSym] === "Account") {
        return value.$jazz.id === this.$jazz.id;
      }
      return false;
    }

    return (
      valueOwner.getRoleOf(this.$jazz.id) === "admin" ||
      valueOwner.getRoleOf(this.$jazz.id) === "manager"
    );
  }

  canAdmin(value: CoValue): boolean {
    const valueOwner = value.$jazz.owner;
    if (!valueOwner) {
      if (value[TypeSym] === "Group") {
        const roleInGroup = (value as Group).getRoleOf(this.$jazz.id);
        return roleInGroup === "admin";
      }
      if (value[TypeSym] === "Account") {
        return value.$jazz.id === this.$jazz.id;
      }
      return false;
    }

    return valueOwner.getRoleOf(this.$jazz.id) === "admin";
  }

  /** @private */
  static async create<A extends Account>(
    this: CoValueClass<A> & typeof Account,
    options: {
      creationProps: { name: string };
      initialAgentSecret?: AgentSecret;
      peers?: Peer[];
      crypto: CryptoProvider;
    },
  ): Promise<A> {
    const { node } = await LocalNode.withNewlyCreatedAccount({
      ...options,
      migration: async (rawAccount, _node, creationProps) => {
        const account = new this({
          fromRaw: rawAccount,
        }) as A;

        await account.applyMigration?.(creationProps);
      },
    });

    return this.fromNode(node) as A;
  }

  static getMe<A extends Account>(this: CoValueClass<A> & typeof Account) {
    return activeAccountContext.get() as A;
  }

  /**
   * @deprecated Use `co.account(...).createAs` instead.
   */
  static async createAs<A extends Account>(
    this: CoValueClass<A> & typeof Account,
    as: Account,
    options: {
      creationProps: { name: string };
    },
  ) {
    // TODO: is there a cleaner way to do this?
    const connectedPeers = cojsonInternals.connectedPeers(
      "creatingAccount",
      "createdAccount",
      { peer1role: "server", peer2role: "client" },
    );

    as.$jazz.localNode.syncManager.addPeer(connectedPeers[1]);

    const account = await this.create<A>({
      creationProps: options.creationProps,
      crypto: as.$jazz.localNode.crypto,
      peers: [connectedPeers[0]],
    });

    await account.$jazz.waitForAllCoValuesSync();

    return account;
  }

  static fromNode<A extends Account>(
    this: CoValueClass<A>,
    node: LocalNode,
  ): A {
    return new this({
      fromRaw: node.expectCurrentAccount("jazz-tools/Account.fromNode"),
    }) as A;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(): object | any[] {
    return {
      $jazz: { id: this.$jazz.id },
    };
  }

  [inspect]() {
    return this.toJSON();
  }

  async applyMigration(creationProps?: AccountCreationProps) {
    await this.migrate(creationProps);

    // if the user has not defined a profile themselves, we create one
    if (this.profile === undefined && creationProps) {
      const profileGroup = RegisteredSchemas["Group"].create({ owner: this });

      this.$jazz.set(
        "profile",
        Profile.create({ name: creationProps.name }, profileGroup) as any,
      );
      profileGroup.addMember("everyone", "reader");
    }

    const profile = this.$jazz.localNode
      .expectCoValueLoaded(this.$jazz.raw.get("profile")!)
      .getCurrentContent() as RawCoMap;

    if (!profile.get("inbox")) {
      const inboxRoot = createInboxRoot(this);
      profile.set("inbox", inboxRoot.id);
      profile.set("inboxInvite", inboxRoot.inviteLink);
    }
  }

  // Placeholder method for subclasses to override
  migrate(creationProps?: AccountCreationProps) {
    creationProps; // To avoid unused parameter warning
  }

  /**
   * Load an `Account`
   * @category Subscription & Loading
   * @deprecated Use `co.account(...).load` instead.
   */
  static load<A extends Account, const R extends RefsToResolve<A> = true>(
    this: CoValueClass<A>,
    id: ID<A>,
    options?: {
      resolve?: RefsToResolveStrict<A, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<A, R> | null> {
    return loadCoValueWithoutMe(this, id, options);
  }

  /**
   * Subscribe to an `Account`, when you have an ID but don't have an `Account` instance yet
   * @category Subscription & Loading
   * @deprecated Use `co.account(...).subscribe` instead.
   */
  static subscribe<A extends Account, const R extends RefsToResolve<A> = true>(
    this: CoValueClass<A>,
    id: ID<A>,
    listener: (value: Resolved<A, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<A extends Account, const R extends RefsToResolve<A> = true>(
    this: CoValueClass<A>,
    id: ID<A>,
    options: SubscribeListenerOptions<A, R>,
    listener: (value: Resolved<A, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: CoValueClass<A>,
    id: ID<A>,
    ...args: SubscribeRestArgs<A, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<A, R>(this, id, options, listener);
  }
}

class AccountJazzApi<A extends Account> extends CoValueJazzApi<A> {
  /**
   * Whether this account is the owner of the local node.
   *
   * @internal
   */
  isLocalNodeOwner: boolean;
  /** @internal */
  sessionID: SessionID | undefined;

  constructor(
    private account: A,
    public raw: RawAccount,
  ) {
    super(account);
    this.isLocalNodeOwner = this.raw.id === this.localNode.getCurrentAgent().id;
    if (this.isLocalNodeOwner) {
      this.sessionID = this.localNode.currentSessionID;
    }
  }

  /**
   * Accounts have no owner. They can be accessed by everyone.
   */
  get owner(): undefined {
    return undefined;
  }

  /**
   * Set the value of a key in the account.
   *
   * @param key The key to set.
   * @param value The value to set.
   *
   * @category Content
   */
  set<K extends "root" | "profile">(
    key: K,
    value: CoFieldInit<NonNullable<A[K]>>,
  ) {
    if (value) {
      let refId = (value as unknown as CoValue).$jazz?.id as
        | CoID<RawCoMap>
        | undefined;
      if (!refId) {
        const descriptor = this.schema[key];
        const coValue = instantiateRefEncodedWithInit(
          descriptor,
          value,
          accountOrGroupToGroup(this.account),
        );
        refId = coValue.$jazz.id as CoID<RawCoMap>;
      }
      this.raw.set(key, refId, "trusting");
    }
  }

  has(key: "root" | "profile"): boolean {
    const entry = this.raw.getRaw(key);
    return entry?.change !== undefined && entry.change.op !== "del";
  }

  /**
   * Get the descriptor for a given key
   * @internal
   */
  getDescriptor(key: string) {
    if (key === "profile") {
      return this.schema.profile;
    } else if (key === "root") {
      return this.schema.root;
    }

    return undefined;
  }

  /**
   * If property `prop` is a `coField.ref(...)`, you can use `account.$jazz.refs.prop` to access
   * the `Ref` instead of the potentially loaded/null value.
   *
   * This allows you to always get the ID or load the value manually.
   *
   * @category Content
   */
  get refs(): {
    profile: RefIfCoValue<Profile> | undefined;
    root: RefIfCoValue<CoMap> | undefined;
  } {
    const profileID = this.raw.get("profile") as unknown as
      | ID<NonNullable<(typeof this.account)["profile"]>>
      | undefined;
    const rootID = this.raw.get("root") as unknown as
      | ID<NonNullable<(typeof this.account)["root"]>>
      | undefined;

    return {
      profile: profileID
        ? (new Ref(
            profileID,
            this.loadedAs,
            this.schema.profile as RefEncoded<
              NonNullable<(typeof this.account)["profile"]> & CoValue
            >,
            this.account,
          ) as unknown as RefIfCoValue<(typeof this.account)["profile"]>)
        : undefined,
      root: rootID
        ? (new Ref(
            rootID,
            this.loadedAs,
            this.schema.root as RefEncoded<
              NonNullable<(typeof this.account)["root"]> & CoValue
            >,
            this.account,
          ) as unknown as RefIfCoValue<(typeof this.account)["root"]>)
        : undefined,
    };
  }

  /** @category Subscription & Loading */
  ensureLoaded<A extends Account, const R extends RefsToResolve<A>>(
    this: AccountJazzApi<A>,
    options: {
      resolve: RefsToResolveStrict<A, R>;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Resolved<A, R>> {
    return ensureCoValueLoaded(this.account as unknown as A, options);
  }

  /** @category Subscription & Loading */
  subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: AccountJazzApi<A>,
    listener: (value: Resolved<A, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: AccountJazzApi<A>,
    options: {
      resolve?: RefsToResolveStrict<A, R>;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: Resolved<A, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: AccountJazzApi<A>,
    ...args: SubscribeRestArgs<A, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this.account, options, listener);
  }

  /**
   * Wait for the `Account` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }

  /**
   * Wait for all the available `CoValues` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForAllCoValuesSync(options?: { timeout?: number }) {
    return this.localNode.syncManager.waitForAllCoValuesSync(options?.timeout);
  }

  /** @internal */
  get schema(): {
    profile: RefEncoded<Profile>;
    root: RefEncoded<CoMap>;
  } {
    return (this.account.constructor as typeof Account)._schema;
  }

  get loadedAs(): Account | AnonymousJazzAgent {
    if (this.isLocalNodeOwner) return this.account;

    const agent = this.localNode.getCurrentAgent();

    if (agent instanceof RawControlledAccount) {
      return coValuesCache.get(agent.account, () =>
        Account.fromRaw(agent.account),
      );
    }

    return new AnonymousJazzAgent(this.localNode);
  }
}

export const AccountAndGroupProxyHandler: ProxyHandler<Account | Group> = {
  get(target, key, receiver) {
    if (key === "profile" || key === "root") {
      const id = target.$jazz.raw.get(key);

      if (id) {
        return accessChildByKey(target, id, key);
      } else {
        return undefined;
      }
    } else {
      return Reflect.get(target, key, receiver);
    }
  },
  set(target, key, value, receiver) {
    if (
      target instanceof Account &&
      (key === "profile" || key === "root") &&
      typeof value === "object" &&
      SchemaInit in value
    ) {
      (target.constructor as typeof Account)._schema ||= {};
      (target.constructor as typeof Account)._schema[key] = value[SchemaInit];
      return true;
    } else if (
      target instanceof Account &&
      (key === "profile" || key === "root")
    ) {
      if (value) {
        target.$jazz.set(key, value);
      }

      return true;
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  defineProperty(target, key, descriptor) {
    if (
      (key === "profile" || key === "root") &&
      typeof descriptor.value === "object" &&
      SchemaInit in descriptor.value
    ) {
      (target.constructor as typeof Account)._schema ||= {};
      (target.constructor as typeof Account)._schema[key] =
        descriptor.value[SchemaInit];
      return true;
    } else {
      return Reflect.defineProperty(target, key, descriptor);
    }
  },
};

export type ControlledAccount = Account & {
  $jazz: {
    raw: RawAccount;
    isLocalNodeOwner: true;
    sessionID: SessionID;
  };
};

/** @category Identity & Permissions */
export function isControlledAccount(
  account: Account,
): account is ControlledAccount {
  return account.$jazz.isLocalNodeOwner;
}

export type AccountClass<Acc extends Account> = CoValueClass<Acc> & {
  fromNode: (typeof Account)["fromNode"];
};

RegisteredSchemas["Account"] = Account;

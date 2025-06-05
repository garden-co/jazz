import {
  AgentSecret,
  CoID,
  ControlledAccount,
  CryptoProvider,
  Everyone,
  InviteSecret,
  LocalNode,
  Peer,
  RawAccount,
  RawCoMap,
  RawCoValue,
  Role,
  SessionID,
  cojsonInternals,
} from "cojson";
import {
  AnonymousJazzAgent,
  CoMap,
  type CoValue,
  CoValueBase,
  CoValueClass,
  CoValueOrZodSchema,
  type Group,
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
  type Schema,
  SchemaInit,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  accessChildByKey,
  activeAccountContext,
  anySchemaToCoSchema,
  coField,
  coValuesCache,
  createInboxRoot,
  ensureCoValueLoaded,
  inspect,
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

type AccountMembers<A extends Account> = [
  {
    id: string | "everyone";
    role: Role;
    ref: Ref<A>;
    account: A;
  },
];

export class AccountInbox extends CoMap {
  inbox? = coField.optional.string;
}

/** @category Identity & Permissions */
export class Account extends CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: "Account";
  declare _raw: RawAccount;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;
  get _schema(): {
    profile: Schema;
    root: Schema;
    inbox: Schema;
  } {
    return (this.constructor as typeof Account)._schema;
  }
  static {
    this._schema = {
      profile: {
        ref: () => Profile,
        optional: false,
      } satisfies RefEncoded<Profile>,
      root: {
        ref: () => RegisteredSchemas["CoMap"],
        optional: true,
      } satisfies RefEncoded<CoMap>,
      inbox: {
        ref: () => AccountInbox,
        optional: true,
      } satisfies RefEncoded<AccountInbox>,
    };
  }

  get _owner(): Account {
    return this as Account;
  }
  get _loadedAs(): Account | AnonymousJazzAgent {
    if (this.isLocalNodeOwner) return this;

    const agent = this._raw.core.node.getCurrentAgent();

    if (agent instanceof ControlledAccount) {
      return coValuesCache.get(agent.account, () =>
        Account.fromRaw(agent.account),
      );
    }

    return new AnonymousJazzAgent(this._raw.core.node);
  }

  declare profile: Profile | null;
  declare root: CoMap | null;
  declare inbox: AccountInbox | null;

  getDescriptor(key: string) {
    if (key === "profile") {
      return this._schema.profile;
    } else if (key === "root") {
      return this._schema.root;
    } else if (key === "inbox") {
      return this._schema.inbox;
    }

    return undefined;
  }

  get _refs(): {
    profile: RefIfCoValue<Profile> | undefined;
    root: RefIfCoValue<CoMap> | undefined;
    inbox: RefIfCoValue<AccountInbox> | undefined;
  } {
    const profileID = this._raw.get("profile") as unknown as
      | ID<NonNullable<this["profile"]>>
      | undefined;
    const rootID = this._raw.get("root") as unknown as
      | ID<NonNullable<this["root"]>>
      | undefined;
    const inboxID = this._raw.get("inbox") as unknown as
      | ID<NonNullable<this["inbox"]>>
      | undefined;

    return {
      profile: profileID
        ? (new Ref(
            profileID,
            this._loadedAs,
            this._schema.profile as RefEncoded<
              NonNullable<this["profile"]> & CoValue
            >,
            this,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          ) as any as RefIfCoValue<this["profile"]>)
        : undefined,
      root: rootID
        ? (new Ref(
            rootID,
            this._loadedAs,
            this._schema.root as RefEncoded<
              NonNullable<this["root"]> & CoValue
            >,
            this,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          ) as any as RefIfCoValue<this["root"]>)
        : undefined,
      inbox: inboxID
        ? (new Ref(
            inboxID,
            this._loadedAs,
            this._schema.inbox as RefEncoded<
              NonNullable<this["inbox"]> & CoValue
            >,
            this,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          ) as any as RefIfCoValue<this["inbox"]>)
        : undefined,
    };
  }

  /**
   * Whether this account is the currently active account.
   */
  get isMe() {
    return activeAccountContext.get().id === this.id;
  }

  /**
   * Whether this account is the owner of the local node.
   */
  isLocalNodeOwner: boolean;
  sessionID: SessionID | undefined;

  constructor(options: { fromRaw: RawAccount }) {
    super();
    if (!("fromRaw" in options)) {
      throw new Error("Can only construct account from raw or with .create()");
    }
    this.isLocalNodeOwner =
      options.fromRaw.id == options.fromRaw.core.node.getCurrentAgent().id;

    Object.defineProperties(this, {
      id: {
        value: options.fromRaw.id,
        enumerable: false,
      },
      _raw: { value: options.fromRaw, enumerable: false },
      _type: { value: "Account", enumerable: false },
    });

    if (this.isLocalNodeOwner) {
      this.sessionID = options.fromRaw.core.node.currentSessionID;
    }

    return new Proxy(this, AccountAndGroupProxyHandler as ProxyHandler<this>);
  }

  myRole(): "admin" | undefined {
    if (this.isLocalNodeOwner) {
      return "admin";
    }
  }

  getRoleOf(member: Everyone | ID<Account> | "me") {
    if (member === "me") {
      return this.isMe ? "admin" : undefined;
    }

    if (member === this.id) {
      return "admin";
    }

    return undefined;
  }

  getParentGroups(): Array<Group> {
    return [];
  }

  get members(): AccountMembers<this> {
    const ref = new Ref<typeof this>(
      this.id,
      this._loadedAs,
      {
        ref: () => this.constructor as AccountClass<typeof this>,
        optional: false,
      },
      this,
    );

    return [{ id: this.id, role: "admin", ref, account: this }];
  }

  canRead(value: CoValue) {
    const role = value._owner.getRoleOf(this.id);

    return (
      role === "admin" ||
      role === "writer" ||
      role === "reader" ||
      role === "writeOnly"
    );
  }

  canWrite(value: CoValue) {
    const role = value._owner.getRoleOf(this.id);

    return role === "admin" || role === "writer" || role === "writeOnly";
  }

  canAdmin(value: CoValue) {
    return value._owner.getRoleOf(this.id) === "admin";
  }

  async acceptInvite<S extends CoValueOrZodSchema>(
    valueID: string,
    inviteSecret: InviteSecret,
    coValueClass: S,
  ): Promise<Resolved<InstanceOrPrimitiveOfSchema<S>, true> | null> {
    if (!this.isLocalNodeOwner) {
      throw new Error("Only a controlled account can accept invites");
    }

    await this._raw.core.node.acceptInvite(
      valueID as unknown as CoID<RawCoValue>,
      inviteSecret,
    );

    return loadCoValue(anySchemaToCoSchema(coValueClass), valueID, {
      loadAs: this,
    }) as Resolved<InstanceOrPrimitiveOfSchema<S>, true> | null;
  }

  /** @private */
  static async create<A extends Account>(
    this: CoValueClass<A> & typeof Account,
    options: {
      creationProps: { name: string };
      initialAgentSecret?: AgentSecret;
      peersToLoadFrom?: Peer[];
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

    as._raw.core.node.syncManager.addPeer(connectedPeers[1]);

    const account = await this.create<A>({
      creationProps: options.creationProps,
      crypto: as._raw.core.node.crypto,
      peersToLoadFrom: [connectedPeers[0]],
    });

    await account.waitForAllCoValuesSync();

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
      id: this.id,
      _type: this._type,
    };
  }

  [inspect]() {
    return this.toJSON();
  }

  async applyMigration(creationProps?: AccountCreationProps) {
    await this.migrate(creationProps);

    if (this.inbox?.inbox === undefined) {
      const inboxGroup = RegisteredSchemas["Group"].create({ owner: this });
      const inboxRoot = createInboxRoot(this);
      console.log("creating inbox in migration");
      this.inbox = AccountInbox.create({ inbox: inboxRoot.id }, inboxGroup);
      inboxGroup.addMember("everyone", "writeOnly");
      console.log("created inbox in migration", this.inbox);
    } else if (this.inbox) {
      if (this.inbox._owner._type !== "Group") {
        throw new Error("Inbox must be owned by a Group", {
          cause: `The inbox of the account "${this.id}" was created with an Account as owner, which is not allowed.`,
        });
      }
    }

    // if the user has not defined a profile themselves, we create one
    if (this.profile === undefined && creationProps) {
      const profileGroup = RegisteredSchemas["Group"].create({ owner: this });

      console.log("creating profile in migration");
      this.profile = Profile.create({ name: creationProps.name }, profileGroup);
      console.log("created profile in migration", this.profile);
      profileGroup.addMember("everyone", "reader");
    } else if (this.profile && creationProps) {
      if (this.profile._owner._type !== "Group") {
        throw new Error("Profile must be owned by a Group", {
          cause: `The profile of the account "${this.id}" was created with an Account as owner, which is not allowed.`,
        });
      }
    }
  }

  // Placeholder method for subclasses to override
  migrate(creationProps?: AccountCreationProps) {
    creationProps; // To avoid unused parameter warning
  }

  /** @category Subscription & Loading */
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

  /** @category Subscription & Loading */
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

  /** @category Subscription & Loading */
  ensureLoaded<A extends Account, const R extends RefsToResolve<A>>(
    this: A,
    options: { resolve: RefsToResolveStrict<A, R> },
  ): Promise<Resolved<A, R>> {
    return ensureCoValueLoaded(this, options);
  }

  /** @category Subscription & Loading */
  subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: A,
    listener: (value: Resolved<A, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: A,
    options: { resolve?: RefsToResolveStrict<A, R> },
    listener: (value: Resolved<A, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<A extends Account, const R extends RefsToResolve<A>>(
    this: A,
    ...args: SubscribeRestArgs<A, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this, options, listener);
  }

  /**
   * Wait for the `Account` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: {
    timeout?: number;
  }) {
    return this._raw.core.waitForSync(options);
  }

  /**
   * Wait for all the available `CoValues` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForAllCoValuesSync(options?: {
    timeout?: number;
  }) {
    return this._raw.core.node.syncManager.waitForAllCoValuesSync(
      options?.timeout,
    );
  }
}

/**
 * For accounts and groups, the values for the `profile`, `root`, and `inbox` keys are their IDs, not the values themselves.
 * Setting any other property will set a key-value mapping where the key is the property name and the value is the property value.
 */
export const AccountAndGroupProxyHandler: ProxyHandler<Account | Group> = {
  get(target, key, receiver) {
    if (key === "profile" || key === "root" || key === "inbox") {
      const id = target._raw.get(key);
      console.log(`get ${key}`, id);

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
    console.log(`set ${key.toString()}`, value?.id, target.myRole());
    if (
      (key === "profile" || key === "root" || key === "inbox") &&
      typeof value === "object" &&
      SchemaInit in value
    ) {
      (target.constructor as typeof CoMap)._schema ||= {};
      (target.constructor as typeof CoMap)._schema[key] = value[SchemaInit];
      return true;
    } else if (key === "inbox") {
      if (value) {
        // The 'trusting' privacy level means that the inbox ID is readable by anyone, allowing other accounts to load this account's inbox ID.
        // FIXME: `trusting` causes getting the inbox ID to fail for everyone (including the setter); however, 'private' (default) allows the setter to read the inbox ID, but nobody else can. No idea why.
        target._raw.set("inbox", value.id as unknown as CoID<RawCoMap>);
      }

      return true;
    } else if (key === "profile") {
      if (value) {
        // The 'trusting' privacy level allows other accounts to load this account's profile ID.
        // This is unlike the account root, whose ID is not visible to other accounts (unless shared out-of-band).
        target._raw.set(
          "profile",
          value.id as unknown as CoID<RawCoMap>,
          "trusting",
        );
      }

      return true;
    } else if (key === "root") {
      if (value) {
        target._raw.set("root", value.id as unknown as CoID<RawCoMap>);
      }
      return true;
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  defineProperty(target, key, descriptor) {
    if (
      (key === "profile" || key === "root" || key === "inbox") &&
      typeof descriptor.value === "object" &&
      SchemaInit in descriptor.value
    ) {
      console.log(`define property ${key}`, descriptor.value);
      (target.constructor as typeof CoMap)._schema ||= {};
      (target.constructor as typeof CoMap)._schema[key] =
        descriptor.value[SchemaInit];
      return true;
    } else {
      return Reflect.defineProperty(target, key, descriptor);
    }
  },
};

/** @category Identity & Permissions */
export function isControlledAccount(account: Account): account is Account & {
  isLocalNodeOwner: true;
  sessionID: SessionID;
  _raw: RawAccount;
} {
  return account.isLocalNodeOwner;
}

export type AccountClass<Acc extends Account> = CoValueClass<Acc> & {
  fromNode: (typeof Account)["fromNode"];
};

RegisteredSchemas["Account"] = Account;

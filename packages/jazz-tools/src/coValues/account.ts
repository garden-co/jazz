import {
  AgentSecret,
  CoID,
  CryptoProvider,
  Everyone,
  InviteSecret,
  LocalNode,
  Peer,
  RawAccount,
  RawCoMap,
  RawCoValue,
  RawControlledAccount,
  Role,
  SessionID,
  cojsonInternals,
} from "cojson";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import {
  AnonymousJazzAgent,
  type CoValue,
  CoValueBase,
  CoValueClass,
  ID,
  Ref,
  type RefEncoded,
  RefIfCoValue,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  type Schema,
  SchemaInit,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  ensureCoValueLoaded,
  inspect,
  loadCoValue,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
  subscriptionsScopes,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { RegisteredAccount } from "../types.js";
import { type CoMap } from "./coMap.js";
import { type Group } from "./group.js";
import { createInboxRoot } from "./inbox.js";
import { Profile } from "./profile.js";
import { RegisteredSchemas } from "./registeredSchemas.js";

export type AccountCreationProps = {
  name: string;
  onboarding?: boolean;
};

/** @category Identity & Permissions */
export class Account extends CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: "Account";
  declare _raw: RawAccount | RawControlledAccount;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;
  get _schema(): {
    profile: Schema;
    root: Schema;
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
    };
  }

  get _owner(): Account {
    return this as Account;
  }
  get _loadedAs(): Account | AnonymousJazzAgent {
    if (this.isLocalNodeOwner) return this;

    const rawAccount = this._raw.core.node.account;

    if (rawAccount instanceof RawAccount) {
      return coValuesCache.get(rawAccount, () => Account.fromRaw(rawAccount));
    }

    return new AnonymousJazzAgent(this._raw.core.node);
  }

  declare profile: Profile | null;
  declare root: CoMap | null;

  get _refs(): {
    profile: RefIfCoValue<Profile> | undefined;
    root: RefIfCoValue<CoMap> | undefined;
  } {
    const profileID = this._raw.get("profile") as unknown as
      | ID<NonNullable<this["profile"]>>
      | undefined;
    const rootID = this._raw.get("root") as unknown as
      | ID<NonNullable<this["root"]>>
      | undefined;

    return {
      profile:
        profileID &&
        (new Ref(
          profileID,
          this._loadedAs,
          this._schema.profile as RefEncoded<
            NonNullable<this["profile"]> & CoValue
          >,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ) as any as RefIfCoValue<this["profile"]>),
      root:
        rootID &&
        (new Ref(
          rootID,
          this._loadedAs,
          this._schema.root as RefEncoded<NonNullable<this["root"]> & CoValue>,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ) as any as RefIfCoValue<this["root"]>),
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

  constructor(options: { fromRaw: RawAccount | RawControlledAccount }) {
    super();
    if (!("fromRaw" in options)) {
      throw new Error("Can only construct account from raw or with .create()");
    }
    this.isLocalNodeOwner =
      options.fromRaw.id == options.fromRaw.core.node.account.id;

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

  get members(): Array<{
    id: ID<RegisteredAccount> | "everyone";
    role: Role;
    ref: Ref<RegisteredAccount> | undefined;
    account: RegisteredAccount | null | undefined;
  }> {
    const ref = new Ref<RegisteredAccount>(this.id, this._loadedAs, {
      ref: () => this.constructor as typeof Account,
      optional: false,
    });

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

  async acceptInvite<V extends CoValue>(
    valueID: ID<V>,
    inviteSecret: InviteSecret,
    coValueClass: CoValueClass<V>,
  ): Promise<Resolved<V, true> | null> {
    if (!this.isLocalNodeOwner) {
      throw new Error("Only a controlled account can accept invites");
    }

    await (this._raw as RawControlledAccount).acceptInvite(
      valueID as unknown as CoID<RawCoValue>,
      inviteSecret,
    );

    return loadCoValue(coValueClass, valueID, {
      loadAs: this,
    });
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
      fromRaw: node.account as RawControlledAccount,
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

    // if the user has not defined a profile themselves, we create one
    if (this.profile === undefined && creationProps) {
      const profileGroup = RegisteredSchemas["Group"].create({ owner: this });

      this.profile = Profile.create({ name: creationProps.name }, profileGroup);
      this.profile._owner.addMember("everyone", "reader");
    } else if (this.profile && creationProps) {
      if (this.profile._owner._type !== "Group") {
        throw new Error("Profile must be owned by a Group", {
          cause: `The profile of the account "${this.id}" was created with an Account as owner, which is not allowed.`,
        });
      }
    }

    const node = this._raw.core.node;
    const profile = node
      .expectCoValueLoaded(this._raw.get("profile")!)
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

export const AccountAndGroupProxyHandler: ProxyHandler<Account | Group> = {
  get(target, key, receiver) {
    if (key === "profile") {
      const ref = target._refs.profile;
      return ref
        ? ref.accessFrom(receiver, "profile")
        : // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (undefined as any);
    } else if (key === "root") {
      const ref = target._refs.root;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return ref ? ref.accessFrom(receiver, "root") : (undefined as any);
    } else {
      return Reflect.get(target, key, receiver);
    }
  },
  set(target, key, value, receiver) {
    if (
      (key === "profile" || key === "root") &&
      typeof value === "object" &&
      SchemaInit in value
    ) {
      (target.constructor as typeof CoMap)._schema ||= {};
      (target.constructor as typeof CoMap)._schema[key] = value[SchemaInit];
      return true;
    } else if (key === "profile") {
      if (value) {
        target._raw.set(
          "profile",
          value.id as unknown as CoID<RawCoMap>,
          "trusting",
        );
      }
      subscriptionsScopes
        .get(receiver)
        ?.onRefAccessedOrSet(target.id, value.id);
      return true;
    } else if (key === "root") {
      if (value) {
        target._raw.set("root", value.id as unknown as CoID<RawCoMap>);
      }
      subscriptionsScopes
        .get(receiver)
        ?.onRefAccessedOrSet(target.id, value.id);
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
  _raw: RawControlledAccount;
} {
  return account.isLocalNodeOwner;
}

export type AccountClass<Acc extends Account> = CoValueClass<Acc> & {
  fromNode: (typeof Account)["fromNode"];
};

RegisteredSchemas["Account"] = Account;

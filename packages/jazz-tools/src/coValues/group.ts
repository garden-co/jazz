import type {
  AccountRole,
  AgentID,
  Everyone,
  RawAccountID,
  RawGroup,
  Role,
} from "cojson";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import type {
  CoValue,
  CoValueClass,
  ID,
  RefEncoded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  SubscribeListenerOptions,
  SubscribeRestArgs,
} from "../internal.js";
import {
  CoValueBase,
  Ref,
  co,
  ensureCoValueLoaded,
  loadCoValueWithoutMe,
  parseGroupCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
} from "../internal.js";
import { RegisteredAccount } from "../types.js";
import { AccountAndGroupProxyHandler, isControlledAccount } from "./account.js";
import { type Account } from "./account.js";
import { type CoMap } from "./coMap.js";
import { type Profile } from "./profile.js";
import { RegisteredSchemas } from "./registeredSchemas.js";

/** @category Identity & Permissions */
export class Group extends CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: "Group";
  static {
    this.prototype._type = "Group";
  }
  declare _raw: RawGroup;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;
  get _schema(): {
    profile: Schema;
    root: Schema;
  } {
    return (this.constructor as typeof Group)._schema;
  }
  static {
    this._schema = {
      profile: "json" satisfies Schema,
      root: "json" satisfies Schema,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
    Object.defineProperty(this.prototype, "_schema", {
      get: () => this._schema,
    });
  }

  declare profile: Profile | null;
  declare root: CoMap | null;

  get _refs(): {
    profile: Ref<Profile> | undefined;
    root: Ref<CoMap> | undefined;
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
          this._schema.profile as RefEncoded<NonNullable<this["profile"]>>,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ) as any as this["profile"] extends Profile
          ? Ref<this["profile"]>
          : never),
      root:
        rootID &&
        (new Ref(
          rootID,
          this._loadedAs,
          this._schema.root as RefEncoded<NonNullable<this["root"]>>,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ) as any as this["root"] extends CoMap ? Ref<this["root"]> : never),
    };
  }

  /** @deprecated Don't use constructor directly, use .create */
  constructor(options: { fromRaw: RawGroup } | { owner: Account | Group }) {
    super();
    let raw: RawGroup;

    if (options && "fromRaw" in options) {
      raw = options.fromRaw;
    } else {
      const initOwner = options.owner;
      if (!initOwner) throw new Error("No owner provided");
      if (initOwner._type === "Account" && isControlledAccount(initOwner)) {
        const rawOwner = initOwner._raw;
        raw = rawOwner.createGroup();
      } else {
        throw new Error("Can only construct group as a controlled account");
      }
    }

    Object.defineProperties(this, {
      id: {
        value: raw.id,
        enumerable: false,
      },
      _raw: { value: raw, enumerable: false },
    });

    return new Proxy(this, AccountAndGroupProxyHandler as ProxyHandler<this>);
  }

  static create<G extends Group>(
    this: CoValueClass<G>,
    options?: { owner: Account } | Account,
  ) {
    return new this(parseGroupCreateOptions(options));
  }

  myRole(): Role | undefined {
    return this._raw.myRole();
  }

  addMember(member: Everyone, role: "writer" | "reader"): void;
  addMember(member: Account, role: AccountRole): void;
  addMember(member: Everyone | Account, role: AccountRole) {
    this._raw.addMember(member === "everyone" ? member : member._raw, role);
  }

  removeMember(member: Everyone | Account) {
    return this._raw.removeMember(member === "everyone" ? member : member._raw);
  }

  get members(): Array<{
    id: ID<RegisteredAccount>;
    role: AccountRole;
    ref: Ref<RegisteredAccount>;
    account: RegisteredAccount;
  }> {
    const members = [];

    const BaseAccountSchema =
      (activeAccountContext.maybeGet()?.constructor as typeof Account) ||
      RegisteredSchemas["Account"];
    const refEncodedAccountSchema = {
      ref: () => BaseAccountSchema,
      optional: false,
    } satisfies RefEncoded<RegisteredAccount>;

    for (const accountID of this._raw.getAllMemberKeysSet()) {
      if (!isAccountID(accountID)) continue;

      const role = this._raw.roleOf(accountID);

      if (
        role === "admin" ||
        role === "writer" ||
        role === "reader" ||
        role === "writeOnly"
      ) {
        const ref = new Ref<RegisteredAccount>(
          accountID as unknown as ID<RegisteredAccount>,
          this._loadedAs,
          refEncodedAccountSchema,
        );
        const accessRef = () => ref.accessFrom(this, "members." + accountID);

        if (!ref.syncLoad()) {
          console.warn("Account not loaded", accountID);
        }

        members.push({
          id: accountID as unknown as ID<Account>,
          role,
          ref,
          get account() {
            // Accounts values are non-nullable because are loaded as dependencies
            return accessRef() as RegisteredAccount;
          },
        });
      }
    }

    return members;
  }

  getRoleOf(member: Everyone | ID<Account> | "me") {
    if (member === "me") {
      return this._raw.roleOf(
        activeAccountContext.get().id as unknown as RawAccountID,
      );
    }

    return this._raw.roleOf(
      member === "everyone" ? member : (member as unknown as RawAccountID),
    );
  }

  getParentGroups(): Array<Group> {
    return this._raw.getParentGroups().map((group) => Group.fromRaw(group));
  }

  extend(
    parent: Group,
    roleMapping?: "reader" | "writer" | "admin" | "inherit",
  ) {
    this._raw.extend(parent._raw, roleMapping);
    return this;
  }

  async revokeExtend(parent: Group) {
    await this._raw.revokeExtend(parent._raw);
    return this;
  }

  /** @category Subscription & Loading */
  static load<G extends Group, const R extends RefsToResolve<G>>(
    this: CoValueClass<G>,
    id: ID<G>,
    options?: { resolve?: RefsToResolveStrict<G, R>; loadAs?: Account },
  ): Promise<Resolved<G, R> | null> {
    return loadCoValueWithoutMe(this, id, options);
  }

  /** @category Subscription & Loading */
  static subscribe<G extends Group, const R extends RefsToResolve<G>>(
    this: CoValueClass<G>,
    id: ID<G>,
    listener: (value: Resolved<G, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<G extends Group, const R extends RefsToResolve<G>>(
    this: CoValueClass<G>,
    id: ID<G>,
    options: SubscribeListenerOptions<G, R>,
    listener: (value: Resolved<G, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<G extends Group, const R extends RefsToResolve<G>>(
    this: CoValueClass<G>,
    id: ID<G>,
    ...args: SubscribeRestArgs<G, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<G, R>(this, id, options, listener);
  }

  /** @category Subscription & Loading */
  ensureLoaded<G extends Group, const R extends RefsToResolve<G>>(
    this: G,
    options?: { resolve?: RefsToResolveStrict<G, R> },
  ): Promise<Resolved<G, R>> {
    return ensureCoValueLoaded(this, options);
  }

  /** @category Subscription & Loading */
  subscribe<G extends Group, const R extends RefsToResolve<G>>(
    this: G,
    listener: (value: Resolved<G, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<G extends Group, const R extends RefsToResolve<G>>(
    this: G,
    options: { resolve?: RefsToResolveStrict<G, R> },
    listener: (value: Resolved<G, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<G extends Group, const R extends RefsToResolve<G>>(
    this: G,
    ...args: SubscribeRestArgs<G, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this, options, listener);
  }

  /**
   * Wait for the `Group` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this._raw.core.waitForSync(options);
  }
}

RegisteredSchemas["Group"] = Group;

export function isAccountID(id: RawAccountID | AgentID): id is RawAccountID {
  return id.startsWith("co_");
}

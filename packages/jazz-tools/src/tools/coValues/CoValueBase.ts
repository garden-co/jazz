import { ControlledAccount, LocalNode, type RawCoValue } from "cojson";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  CoValueLoadingState,
  ID,
  RegisteredSchemas,
  type SubscriptionScope,
  coValueClassFromCoValueClassOrSchema,
  coValuesCache,
  inspect,
  unstable_mergeBranch,
} from "../internal.js";
import { Group, TypeSym } from "../internal.js";

/** @internal */
export abstract class CoValueBase implements CoValue {
  declare [TypeSym]: string;

  declare abstract $jazz: CoValueJazzApi<this>;
  declare $isLoaded: true;

  constructor() {
    Object.defineProperties(this, {
      $isLoaded: { value: true, enumerable: false },
    });
  }

  /** @category Internals */
  static fromRaw<V extends CoValue>(this: CoValueClass<V>, raw: RawCoValue): V {
    return new this({ fromRaw: raw });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(): object | any[] | string {
    return {
      id: this.$jazz.id,
      type: this[TypeSym],
      error: "unknown CoValue class",
    };
  }

  [inspect]() {
    return this.toJSON();
  }
}

export abstract class CoValueJazzApi<V extends CoValue> {
  /** @category Internals */
  declare _instanceID: string;
  declare _subscriptionScope: SubscriptionScope<CoValue> | undefined;

  constructor(private coValue: V) {
    Object.defineProperty(this, "_instanceID", {
      value: `instance-${Math.random().toString(36).slice(2)}`,
      enumerable: false,
    });
  }

  get id(): string {
    const sourceId = this.raw.core.getCurrentBranchSourceId();

    if (sourceId) {
      return sourceId;
    }

    return this.raw.id;
  }

  get loadingState(): typeof CoValueLoadingState.LOADED {
    return CoValueLoadingState.LOADED;
  }

  abstract get raw(): RawCoValue;
  abstract get owner(): Group | undefined;

  /** @internal */
  get localNode(): LocalNode {
    return this.raw.core.node;
  }

  /** @private */
  get loadedAs() {
    const agent = this.localNode.getCurrentAgent();

    if (agent instanceof ControlledAccount) {
      return coValuesCache.get(agent.account, () =>
        coValueClassFromCoValueClassOrSchema(
          RegisteredSchemas["Account"],
        ).fromRaw(agent.account),
      );
    }

    return new AnonymousJazzAgent(this.localNode);
  }

  /**
   * The timestamp of the creation time of the CoValue
   *
   * @category Content
   */
  get createdAt(): number {
    const createdAt = this.raw.core.verified.header.meta?.createdAt;

    if (typeof createdAt === "string") {
      return new Date(createdAt).getTime();
    }

    return this.raw.core.earliestTxMadeAt;
  }

  /**
   * The timestamp of the last updated time of the CoValue
   *
   * Returns the creation time if there are no updates.
   *
   * @category Content
   */
  get lastUpdatedAt(): number {
    const value = this.raw.core.latestTxMadeAt;

    if (value === 0) {
      return this.createdAt;
    }

    return value;
  }

  /**
   * The name of the active branch of the CoValue
   */
  get branchName(): string | undefined {
    const subscriptionScope = this._subscriptionScope;

    return subscriptionScope?.unstable_branch?.name;
  }

  get isBranched(): boolean {
    const subscriptionScope = this._subscriptionScope;

    return Boolean(subscriptionScope?.unstable_branch);
  }

  /**
   * Deeply merge the current branch into the main CoValues.
   *
   * Doesn't have any effect when there are no changes to merge, or the current CoValue is not a branch
   */
  unstable_merge() {
    const subscriptionScope = this._subscriptionScope;

    if (!subscriptionScope) {
      return;
    }

    unstable_mergeBranch(subscriptionScope);
  }
}

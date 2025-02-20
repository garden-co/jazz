import type {
  CoValueUniqueness,
  CojsonInternalTypes,
  RawCoValue,
} from "cojson";
import { RawAccount } from "cojson";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import { AnonymousJazzAgent } from "../implementation/anonymousJazzAgent.js";
import type { DeeplyLoaded, DepthsIn } from "../internal.js";
import {
  Ref,
  SubscriptionScope,
  inspect,
  subscriptionsScopes,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { type Account } from "./account.js";
import { fulfillsDepth } from "./deepLoading.js";
import { type Group } from "./group.js";
import { RegisteredSchemas } from "./registeredSchemas.js";

/** @category Abstract interfaces */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface CoValueClass<Value extends CoValue = CoValue> {
  /** @ignore */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  new (...args: any[]): Value;
}

export interface CoValueFromRaw<V extends CoValue> {
  fromRaw(raw: V["_raw"]): V;
}

/** @category Abstract interfaces */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface CoValue {
  /** @category Content */
  readonly id: ID<this>;
  /** @category Type Helpers */
  _type: string;
  /** @category Collaboration */
  _owner: Account | Group;
  /** @category Internals */
  _raw: RawCoValue;
  /** @internal */
  readonly _loadedAs: Account | AnonymousJazzAgent;
  /** @category Stringifying & Inspection */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(key?: string, seenAbove?: ID<CoValue>[]): any[] | object | string;
  /** @category Stringifying & Inspection */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [inspect](): any;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isCoValue(value: any): value is CoValue {
  return value && value._type !== undefined;
}

export function isCoValueClass<V extends CoValue>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  value: any,
): value is CoValueClass<V> & CoValueFromRaw<V> {
  return typeof value === "function" && value.fromRaw !== undefined;
}

/**
 * IDs are unique identifiers for `CoValue`s.
 * Can be used with a type argument to refer to a specific `CoValue` type.
 *
 * @example
 *
 * ```ts
 * type AccountID = ID<Account>;
 * ```
 *
 * @category CoValues
 */
export type ID<T> = CojsonInternalTypes.RawCoID & IDMarker<T>;

type IDMarker<out T> = { __type(_: never): T };

/** @internal */
export class CoValueBase implements CoValue {
  declare id: ID<this>;
  declare _type: string;
  declare _raw: RawCoValue;
  /** @category Internals */
  declare _instanceID: string;

  get _owner(): Account | Group {
    const owner =
      this._raw.group instanceof RawAccount
        ? RegisteredSchemas["Account"].fromRaw(this._raw.group)
        : RegisteredSchemas["Group"].fromRaw(this._raw.group);

    const subScope = subscriptionsScopes.get(this);
    if (subScope) {
      subScope.onRefAccessedOrSet(this.id, owner.id);
      subscriptionsScopes.set(owner, subScope);
    }

    return owner;
  }

  /** @private */
  get _loadedAs() {
    const rawAccount = this._raw.core.node.account;

    if (rawAccount instanceof RawAccount) {
      return coValuesCache.get(rawAccount, () =>
        RegisteredSchemas["Account"].fromRaw(rawAccount),
      );
    }

    return new AnonymousJazzAgent(this._raw.core.node);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(..._args: any) {
    Object.defineProperty(this, "_instanceID", {
      value: `instance-${Math.random().toString(36).slice(2)}`,
      enumerable: false,
    });
  }

  /** @category Internals */
  static fromRaw<V extends CoValue>(this: CoValueClass<V>, raw: RawCoValue): V {
    return new this({ fromRaw: raw });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(): object | any[] | string {
    return {
      id: this.id,
      type: this._type,
      error: "unknown CoValue class",
    };
  }

  [inspect]() {
    return this.toJSON();
  }

  /** @category Type Helpers */
  castAs<Cl extends CoValueClass & CoValueFromRaw<CoValue>>(
    cl: Cl,
  ): InstanceType<Cl> {
    const casted = cl.fromRaw(this._raw) as InstanceType<Cl>;
    const subscriptionScope = subscriptionsScopes.get(this);
    if (subscriptionScope) {
      subscriptionsScopes.set(casted, subscriptionScope);
    }
    return casted;
  }
}

export function loadCoValueWithoutMe<V extends CoValue, Depth>(
  cls: CoValueClass<V>,
  id: ID<V>,
  asOrDepth: Account | AnonymousJazzAgent | (Depth & DepthsIn<V>),
  depth?: Depth & DepthsIn<V>,
) {
  if (isAccountInstance(asOrDepth) || isAnonymousAgentInstance(asOrDepth)) {
    if (!depth) {
      throw new Error(
        "Depth is required when loading a CoValue as an Account or AnonymousJazzAgent",
      );
    }
    return loadCoValue(cls, id, asOrDepth, depth);
  }

  return loadCoValue(cls, id, activeAccountContext.get(), asOrDepth);
}

export function loadCoValue<V extends CoValue, Depth>(
  cls: CoValueClass<V>,
  id: ID<V>,
  as: Account | AnonymousJazzAgent,
  depth: Depth & DepthsIn<V>,
): Promise<DeeplyLoaded<V, Depth> | undefined> {
  return new Promise((resolve) => {
    subscribeToCoValue(
      cls,
      id,
      as,
      depth,
      (value, unsubscribe) => {
        resolve(value);
        unsubscribe();
      },
      () => {
        resolve(undefined);
      },
    );
  });
}

export async function ensureCoValueLoaded<V extends CoValue, Depth>(
  existing: V,
  depth: Depth & DepthsIn<V>,
): Promise<DeeplyLoaded<V, Depth>> {
  const response = await loadCoValue(
    existing.constructor as CoValueClass<V>,
    existing.id,
    existing._loadedAs,
    depth,
  );

  if (!response) {
    throw new Error("Failed to deeply load CoValue " + existing.id);
  }

  return response;
}

export function subscribeToCoValueWithoutMe<V extends CoValue, Depth>(
  cls: CoValueClass<V>,
  id: ID<V>,
  asOrDepth: Account | AnonymousJazzAgent | (Depth & DepthsIn<V>),
  depthOrListener:
    | (Depth & DepthsIn<V>)
    | ((value: DeeplyLoaded<V, Depth>) => void),
  listener?: (value: DeeplyLoaded<V, Depth>) => void,
) {
  if (isAccountInstance(asOrDepth) || isAnonymousAgentInstance(asOrDepth)) {
    if (typeof depthOrListener !== "function") {
      return subscribeToCoValue<V, Depth>(
        cls,
        id,
        asOrDepth,
        depthOrListener,
        listener!,
      );
    }
    throw new Error("Invalid arguments");
  }

  if (typeof depthOrListener !== "function") {
    throw new Error("Invalid arguments");
  }

  return subscribeToCoValue<V, Depth>(
    cls,
    id,
    activeAccountContext.get(),
    asOrDepth,
    depthOrListener,
  );
}

export function subscribeToCoValue<V extends CoValue, Depth>(
  cls: CoValueClass<V>,
  id: ID<V>,
  as: Account | AnonymousJazzAgent,
  depth: Depth & DepthsIn<V>,
  listener: (value: DeeplyLoaded<V, Depth>, unsubscribe: () => void) => void,
  onUnavailable?: () => void,
  syncResolution?: boolean,
): () => void {
  const ref = new Ref(id, as, { ref: cls, optional: false });

  let unsubscribed = false;
  let unsubscribe: (() => void) | undefined;

  function subscribe(value: V | undefined) {
    if (!value) {
      onUnavailable && onUnavailable();
      return;
    }
    if (unsubscribed) return;
    const subscription = new SubscriptionScope(
      value,
      cls as CoValueClass<V> & CoValueFromRaw<V>,
      (update, subscription) => {
        if (fulfillsDepth(depth, update)) {
          listener(
            update as DeeplyLoaded<V, Depth>,
            subscription.unsubscribeAll,
          );
        }
      },
    );

    unsubscribe = subscription.unsubscribeAll;
  }

  const sync = syncResolution ? ref.syncLoad() : undefined;

  if (sync) {
    subscribe(sync);
  } else {
    ref
      .load()
      .then((value) => subscribe(value))
      .catch((e) => {
        console.error("Failed to load / subscribe to CoValue", e);
        onUnavailable?.();
      });
  }

  return function unsubscribeAtAnyPoint() {
    unsubscribed = true;
    unsubscribe && unsubscribe();
  };
}

export function createCoValueObservable<V extends CoValue, Depth>(options?: {
  syncResolution?: boolean;
}) {
  let currentValue: DeeplyLoaded<V, Depth> | undefined | null = undefined;
  let subscriberCount = 0;

  function subscribe(
    cls: CoValueClass<V>,
    id: ID<V>,
    as: Account | AnonymousJazzAgent,
    depth: Depth & DepthsIn<V>,
    listener: () => void,
    onUnavailable?: () => void,
  ) {
    subscriberCount++;

    const unsubscribe = subscribeToCoValue(
      cls,
      id,
      as,
      depth,
      (value) => {
        currentValue = value;
        listener();
      },
      () => {
        currentValue = null;
        onUnavailable?.();
      },
      options?.syncResolution,
    );

    return () => {
      unsubscribe();
      subscriberCount--;
      if (subscriberCount === 0) {
        currentValue = undefined;
      }
    };
  }

  const observable = {
    getCurrentValue: () => currentValue,
    subscribe,
  };

  return observable;
}

export function subscribeToExistingCoValue<V extends CoValue, Depth>(
  existing: V,
  depth: Depth & DepthsIn<V>,
  listener: (value: DeeplyLoaded<V, Depth>) => void,
): () => void {
  return subscribeToCoValue(
    existing.constructor as CoValueClass<V>,
    existing.id,
    existing._loadedAs,
    depth,
    listener,
  );
}

export function isAccountInstance(instance: unknown): instance is Account {
  if (typeof instance !== "object" || instance === null) {
    return false;
  }

  return "_type" in instance && instance._type === "Account";
}

export function isAnonymousAgentInstance(
  instance: unknown,
): instance is AnonymousJazzAgent {
  if (typeof instance !== "object" || instance === null) {
    return false;
  }

  return "_type" in instance && instance._type === "Anonymous";
}

export function parseCoValueCreateOptions(
  options:
    | {
        owner?: Account | Group;
        unique?: CoValueUniqueness["uniqueness"];
      }
    | Account
    | Group
    | undefined,
) {
  const Group = RegisteredSchemas["Group"];

  if (!options) {
    return { owner: Group.create(), uniqueness: undefined };
  }

  if ("_type" in options) {
    if (options._type === "Account" || options._type === "Group") {
      return { owner: options, uniqueness: undefined };
    }
  }

  const uniqueness = options.unique
    ? { uniqueness: options.unique }
    : undefined;

  return {
    owner: options.owner ?? Group.create(),
    uniqueness,
  };
}

export function parseGroupCreateOptions(
  options:
    | {
        owner?: Account;
      }
    | Account
    | undefined,
) {
  if (!options) {
    return { owner: activeAccountContext.get() };
  }

  return "_type" in options && isAccountInstance(options)
    ? { owner: options }
    : { owner: options.owner ?? activeAccountContext.get() };
}

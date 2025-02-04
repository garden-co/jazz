import type {
  CoValueUniqueness,
  CojsonInternalTypes,
  RawCoValue,
} from "cojson";
import { RawAccount } from "cojson";
import { activeAccountContext } from "../implementation/activeAccountContext.js";
import { AnonymousJazzAgent } from "../implementation/anonymousJazzAgent.js";
import {
  Ref,
  SubscriptionScope,
  inspect,
  subscriptionsScopes,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { type Account } from "./account.js";
import { RefsToResolve, Resolved, fulfillsDepth } from "./deepLoading.js";
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

export function loadCoValueWithoutMe<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<V>,
  options?: { resolve?: R; loadAs?: Account | AnonymousJazzAgent },
) {
  return loadCoValue(cls, id, {
    ...options,
    loadAs: options?.loadAs ?? activeAccountContext.get(),
  });
}

export function loadCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<V>,
  options: { resolve?: R; loadAs: Account | AnonymousJazzAgent },
): Promise<Resolved<V, R> | undefined> {
  return new Promise((resolve) => {
    subscribeToCoValue<V, R>(
      cls,
      id,
      options,
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

export function ensureCoValueLoaded<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  existing: V,
  options?: { resolve?: R } | undefined,
): Promise<Resolved<V, R> | undefined> {
  return loadCoValue(existing.constructor as CoValueClass<V>, existing.id, {
    loadAs: existing._loadedAs,
    resolve: options?.resolve,
  });
}

type SubscribeListener<V extends CoValue, R extends RefsToResolve<V>> = (
  value: Resolved<V, R>,
  unsubscribe: () => void,
) => void;

export type SubscribeRestArgs<V extends CoValue, R extends RefsToResolve<V>> =
  | [
      options: { resolve?: R; loadAs?: Account | AnonymousJazzAgent },
      listener: SubscribeListener<V, R>,
    ]
  | [listener: SubscribeListener<V, R>];

export function parseSubscribeRestArgs<
  V extends CoValue,
  R extends RefsToResolve<V>,
>(
  args: SubscribeRestArgs<V, R>,
): { options: { resolve?: R }; listener: SubscribeListener<V, R> } {
  if (args.length === 2) {
    if (
      typeof args[0] === "object" &&
      "resolve" in args[0] &&
      typeof args[1] === "function"
    ) {
      return { options: { resolve: args[0].resolve }, listener: args[1] };
    } else {
      throw new Error("Invalid arguments");
    }
  } else {
    if (typeof args[0] === "function") {
      return { options: {}, listener: args[0] };
    } else {
      throw new Error("Invalid arguments");
    }
  }
}

export function subscribeToCoValueWithoutMe<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<V>,
  options: { resolve?: R; loadAs?: Account | AnonymousJazzAgent },
  listener: SubscribeListener<V, R>,
) {
  return subscribeToCoValue(
    cls,
    id,
    { ...options, loadAs: options.loadAs ?? activeAccountContext.get() },
    listener,
  );
}

export function subscribeToCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<V>,
  options: { resolve?: R; loadAs: Account | AnonymousJazzAgent },
  listener: SubscribeListener<V, R>,
  onUnavailable?: () => void,
  syncResolution?: boolean,
): () => void {
  const ref = new Ref(id, options.loadAs, { ref: cls, optional: false });

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
        if (fulfillsDepth(options.resolve, update)) {
          listener(update as Resolved<V, R>, subscription.unsubscribeAll);
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
      });
  }

  return function unsubscribeAtAnyPoint() {
    unsubscribed = true;
    unsubscribe && unsubscribe();
  };
}

export function createCoValueObservable<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(observableOptions?: {
  syncResolution?: boolean;
}) {
  let currentValue: Resolved<V, R> | undefined = undefined;
  let subscriberCount = 0;

  function subscribe(
    cls: CoValueClass<V>,
    id: ID<V>,
    options: { loadAs: Account | AnonymousJazzAgent; resolve?: R },
    listener: () => void,
    onUnavailable?: () => void,
  ) {
    subscriberCount++;

    const unsubscribe = subscribeToCoValue(
      cls,
      id,
      options,
      (value) => {
        currentValue = value;
        listener();
      },
      onUnavailable,
      observableOptions?.syncResolution,
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

export function subscribeToExistingCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  existing: V,
  options: { resolve?: R } | undefined,
  listener: SubscribeListener<V, R>,
): () => void {
  return subscribeToCoValue(
    existing.constructor as CoValueClass<V>,
    existing.id,
    { loadAs: existing._loadedAs, resolve: options?.resolve },
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

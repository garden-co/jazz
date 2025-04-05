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
import {
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  fulfillsDepth,
} from "./deepLoading.js";
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
  id: ID<CoValue>,
  options?: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs?: Account | AnonymousJazzAgent;
  },
): Promise<Resolved<V, R> | null> {
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
  id: ID<CoValue>,
  options: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs: Account | AnonymousJazzAgent;
  },
): Promise<Resolved<V, R> | null> {
  return new Promise((resolve) => {
    subscribeToCoValue<V, R>(
      cls,
      id,
      {
        resolve: options.resolve,
        loadAs: options.loadAs,
        onUnavailable: () => {
          resolve(null);
        },
        onUnauthorized: () => {
          resolve(null);
        },
      },
      (value, unsubscribe) => {
        resolve(value);
        unsubscribe();
      },
    );
  });
}

export async function ensureCoValueLoaded<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  existing: V,
  options?: { resolve?: RefsToResolveStrict<V, R> } | undefined,
): Promise<Resolved<V, R>> {
  const response = await loadCoValue(
    existing.constructor as CoValueClass<V>,
    existing.id,
    {
      loadAs: existing._loadedAs,
      resolve: options?.resolve,
    },
  );

  if (!response) {
    throw new Error("Failed to deeply load CoValue " + existing.id);
  }

  return response;
}

type SubscribeListener<V extends CoValue, R extends RefsToResolve<V>> = (
  value: Resolved<V, R>,
  unsubscribe: () => void,
) => void;

export type SubscribeListenerOptions<
  V extends CoValue,
  R extends RefsToResolve<V>,
> = {
  resolve?: RefsToResolveStrict<V, R>;
  loadAs?: Account | AnonymousJazzAgent;
  onUnauthorized?: () => void;
  onUnavailable?: () => void;
};

export type SubscribeRestArgs<V extends CoValue, R extends RefsToResolve<V>> =
  | [options: SubscribeListenerOptions<V, R>, listener: SubscribeListener<V, R>]
  | [listener: SubscribeListener<V, R>];

export function parseSubscribeRestArgs<
  V extends CoValue,
  R extends RefsToResolve<V>,
>(
  args: SubscribeRestArgs<V, R>,
): {
  options: SubscribeListenerOptions<V, R>;
  listener: SubscribeListener<V, R>;
} {
  if (args.length === 2) {
    if (
      typeof args[0] === "object" &&
      args[0] &&
      typeof args[1] === "function"
    ) {
      return {
        options: {
          resolve: args[0].resolve,
          loadAs: args[0].loadAs,
          onUnauthorized: args[0].onUnauthorized,
          onUnavailable: args[0].onUnavailable,
        },
        listener: args[1],
      };
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
  id: ID<CoValue>,
  options: SubscribeListenerOptions<V, R>,
  listener: SubscribeListener<V, R>,
) {
  return subscribeToCoValue(
    cls,
    id,
    {
      ...options,
      loadAs: options.loadAs ?? activeAccountContext.get(),
    },
    listener,
  );
}

export function subscribeToCoValue<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  cls: CoValueClass<V>,
  id: ID<CoValue>,
  options: {
    resolve?: RefsToResolveStrict<V, R>;
    loadAs: Account | AnonymousJazzAgent;
    onUnavailable?: () => void;
    onUnauthorized?: () => void;
    syncResolution?: boolean;
  },
  listener: SubscribeListener<V, R>,
): () => void {
  const ref = new Ref(id, options.loadAs, { ref: cls, optional: false });

  let unsubscribed = false;
  let unsubscribe: (() => void) | undefined;

  function subscribe() {
    const value = ref.getValueWithoutAccessCheck();

    if (!value) {
      options.onUnavailable?.();
      return;
    }

    if (unsubscribed) return;

    const subscription = new SubscriptionScope(
      value,
      cls as CoValueClass<V> & CoValueFromRaw<V>,
      (update, subscription) => {
        if (subscription.syncResolution) return;

        if (!ref.hasReadAccess()) {
          options.onUnauthorized?.();
          return;
        }

        let result;

        try {
          subscription.syncResolution = true;
          result = fulfillsDepth(options.resolve, update);
        } catch (e) {
          console.error(
            "Failed to load / subscribe to CoValue",
            e,
            e instanceof Error ? e.stack : undefined,
          );
          options.onUnavailable?.();
          return;
        } finally {
          subscription.syncResolution = false;
        }

        if (result === "unauthorized") {
          options.onUnauthorized?.();
          return;
        }

        if (result === "fulfilled") {
          listener(update as Resolved<V, R>, subscription.unsubscribeAll);
        }
      },
    );

    unsubscribe = subscription.unsubscribeAll;
  }

  const sync = options.syncResolution ? ref.syncLoad() : undefined;

  if (sync) {
    subscribe();
  } else {
    ref
      .load()
      .then(() => subscribe())
      .catch((e) => {
        console.error(
          "Failed to load / subscribe to CoValue",
          e,
          e instanceof Error ? e.stack : undefined,
        );
        options.onUnavailable?.();
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
>() {
  let currentValue: Resolved<V, R> | undefined | null = undefined;
  let subscriberCount = 0;

  function subscribe(
    cls: CoValueClass<V>,
    id: ID<CoValue>,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      resolve?: RefsToResolveStrict<V, R>;
      onUnavailable?: () => void;
      onUnauthorized?: () => void;
      syncResolution?: boolean;
    },
    listener: () => void,
  ) {
    subscriberCount++;

    const unsubscribe = subscribeToCoValue(
      cls,
      id,
      {
        loadAs: options.loadAs,
        resolve: options.resolve,
        onUnavailable: () => {
          currentValue = null;
          options.onUnavailable?.();
        },
        onUnauthorized: () => {
          currentValue = null;
          options.onUnauthorized?.();
        },
        syncResolution: options.syncResolution,
      },
      (value) => {
        currentValue = value;
        listener();
      },
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
  options:
    | {
        resolve?: RefsToResolveStrict<V, R>;
        onUnavailable?: () => void;
        onUnauthorized?: () => void;
      }
    | undefined,
  listener: SubscribeListener<V, R>,
): () => void {
  return subscribeToCoValue(
    existing.constructor as CoValueClass<V>,
    existing.id,
    {
      loadAs: existing._loadedAs,
      resolve: options?.resolve,
      onUnavailable: options?.onUnavailable,
      onUnauthorized: options?.onUnauthorized,
    },
    listener,
  );
}

export function waitForCoValueCondition<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>(
  existing: V,
  options: {
    resolve?: RefsToResolveStrict<V, R>;
    onUnavailable?: () => void;
    onUnauthorized?: () => void;
  },
  conditionFn: (value: V) => boolean,
  timeoutMs = 15000,
): Promise<V> {
  return new Promise((resolve, reject) => {
    let aborted = false;
    let unsubscribe = () => {};

    const abort = () => {
      aborted = true;
      unsubscribe();
      clearTimeout(timeout);
    };

    subscribeToCoValue(
      existing.constructor as CoValueClass<V>,
      existing.id,
      {
        loadAs: existing._loadedAs,
        ...options,
      },
      (value, unsubscribeParam) => {
        unsubscribe = unsubscribeParam;
        if (aborted) return;

        if (conditionFn(value)) {
          abort();
          resolve(value);
        }
      },
    );

    const timeout = setTimeout(() => {
      abort();
      reject(new Error("Timeout waiting for CoValue condition"));
    }, timeoutMs);
  });
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

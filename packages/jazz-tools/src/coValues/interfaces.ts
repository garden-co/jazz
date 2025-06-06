import type { CoValueUniqueness, RawCoValue } from "cojson";
import {
  type Account,
  AnonymousJazzAgent,
  CoValueOrZodSchema,
  type Group,
  Loaded,
  RefsToResolve,
  RefsToResolveStrict,
  RegisteredSchemas,
  ResolveQuery,
  ResolveQueryStrict,
  Resolved,
  SubscriptionScope,
  type SubscriptionValue,
  activeAccountContext,
  anySchemaToCoSchema,
  inspect,
} from "../internal.js";

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
  _subscriptionScope?: SubscriptionScope<this>;

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
export type ID<T> = string;

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
        syncResolution: true,
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
  const loadAs = options.loadAs ?? activeAccountContext.get();
  const node = "node" in loadAs ? loadAs.node : loadAs._raw.core.node;

  const resolve = options.resolve ?? true;

  let unsubscribed = false;

  const rootNode = new SubscriptionScope<V>(node, resolve, id as ID<V>, {
    ref: cls,
    optional: false,
  });

  const handleUpdate = (value: SubscriptionValue<V, any>) => {
    if (unsubscribed) return;

    if (value.type === "unavailable") {
      options.onUnavailable?.();

      console.error(value.toString());
    } else if (value.type === "unauthorized") {
      options.onUnauthorized?.();

      console.error(value.toString());
    } else if (value.type === "loaded") {
      listener(value.value as Resolved<V, R>, unsubscribe);
    }
  };

  let shouldDefer = !options.syncResolution;

  rootNode.setListener((value) => {
    if (shouldDefer) {
      shouldDefer = false;
      Promise.resolve().then(() => {
        handleUpdate(value);
      });
    } else {
      handleUpdate(value);
    }
  });

  function unsubscribe() {
    unsubscribed = true;
    rootNode.destroy();
  }

  return unsubscribe;
}

/**
 * @deprecated Used for the React integration in the past, but we moved to use SubscriptionScope directly.
 *
 * Going to be removed in the next minor version.
 */
export function createCoValueObservable<
  S extends CoValueOrZodSchema,
  const R extends ResolveQuery<S>,
>(initialValue: undefined | null = undefined) {
  let currentValue: Loaded<S, R> | undefined | null = initialValue;
  let subscriberCount = 0;

  function subscribe(
    cls: S,
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      resolve?: ResolveQueryStrict<S, R>;
      onUnavailable?: () => void;
      onUnauthorized?: () => void;
      syncResolution?: boolean;
    },
    listener: () => void,
  ) {
    subscriberCount++;

    const unsubscribe = subscribeToCoValue(
      anySchemaToCoSchema(cls),
      id,
      {
        loadAs: options.loadAs,
        resolve: options.resolve as any,
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
        currentValue = value as Loaded<S, R>;
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

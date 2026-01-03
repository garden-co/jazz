import React, {
  useCallback,
  useMemo,
  useRef,
  useSyncExternalStore,
} from "react";
import {
  Account,
  AnonymousJazzAgent,
  captureStack,
  CoValue,
  CoValueClassOrSchema,
  Loaded,
  MaybeLoaded,
  ResolveQuery,
  ResolveQueryStrict,
  SchemaResolveQuery,
  SubscriptionScope,
} from "jazz-tools";
import { useJazzContextManager, useAgent } from "./hooks.js";
import { use } from "./use.js";

/**
 * A subscription definition for a single CoValue in the useSuspenseMultiCoState hook.
 *
 * @typeParam S - The schema or class of the CoValue
 * @typeParam R - The resolve query type for nested loading
 */
export type CoSubscription<
  S extends CoValueClassOrSchema = CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  R extends ResolveQuery<S> = SchemaResolveQuery<S>,
> = {
  /** The CoValue schema or class constructor */
  schema: S;
  /** The ID of the CoValue to subscribe to. If `undefined` or `null`, returns `null` for this entry */
  id: string | undefined | null;
  /** Optional resolve query to specify which nested CoValues to load */
  resolve?: ResolveQueryStrict<S, R>;
};

/**
 * Helper type to extract the Loaded type from a subscription definition.
 * Returns `null` if the subscription ID is not a string.
 */
type LoadedFromSubscription<Sub> = Sub extends CoSubscription<infer S, infer R>
  ? Sub["id"] extends string
    ? Loaded<S, R>
    : null
  : never;

/**
 * Maps a tuple of CoSubscription definitions to a tuple of their Loaded types.
 * Each position preserves the type based on its schema and resolve query.
 *
 * @example
 * ```typescript
 * type Result = UseSuspenseMultiCoStateResult<[
 *   CoSubscription<typeof Project, true>,
 *   CoSubscription<typeof Task, { assignee: true }>
 * ]>;
 * // Result = [Loaded<typeof Project>, Loaded<typeof Task, { assignee: true }>]
 * ```
 */
export type UseSuspenseMultiCoStateResult<
  T extends readonly CoSubscription<any, any>[],
> = {
  -readonly [K in keyof T]: LoadedFromSubscription<T[K]>;
};

/**
 * Helper type to extract the MaybeLoaded type from a subscription definition.
 * Returns `null` if the subscription ID is not a string.
 */
type MaybeLoadedFromSubscription<Sub> = Sub extends CoSubscription<
  infer S,
  infer R
>
  ? Sub["id"] extends string
    ? MaybeLoaded<Loaded<S, R>>
    : null
  : never;

/**
 * Maps a tuple of CoSubscription definitions to a tuple of their MaybeLoaded types.
 * Each position preserves the type based on its schema and resolve query.
 *
 * @example
 * ```typescript
 * type Result = UseMultiCoStateResult<[
 *   CoSubscription<typeof Project, true>,
 *   CoSubscription<typeof Task, { assignee: true }>
 * ]>;
 * // Result = [MaybeLoaded<Loaded<typeof Project>>, MaybeLoaded<Loaded<typeof Task, { assignee: true }>>]
 * ```
 */
export type UseMultiCoStateResult<
  T extends readonly CoSubscription<any, any>[],
> = {
  -readonly [K in keyof T]: MaybeLoadedFromSubscription<T[K]>;
};

/**
 * Gets the resolve query from a schema, falling back to the schema's default or `true`.
 */
function getResolveQuery(
  Schema: CoValueClassOrSchema,
  resolveQuery?: ResolveQuery<any>,
): ResolveQuery<any> {
  if (resolveQuery) {
    return resolveQuery;
  }
  if ("resolveQuery" in Schema) {
    return Schema.resolveQuery;
  }
  return true;
}

/**
 * Tracked state for a single subscription entry.
 */
interface SubscriptionEntry {
  subscription: SubscriptionScope<CoValue> | null;
  schema: CoValueClassOrSchema;
  id: string | undefined | null;
  resolve: ResolveQuery<any> | undefined;
}

/**
 * Tracked state for the entire subscriptions array.
 */
interface SubscriptionsState {
  entries: SubscriptionEntry[];
  contextManager: ReturnType<typeof useJazzContextManager>;
  agent: AnonymousJazzAgent | Loaded<any, true>;
}

/**
 * Internal hook that manages an array of SubscriptionScope instances.
 *
 * - Uses a ref to track subscriptions by index
 * - Detects changes by comparing schema/id/resolve per entry
 * - Creates new subscriptions via SubscriptionScopeCache.getOrCreate()
 * - Returns null for entries with undefined/null IDs
 */
export function useMultiCoStateSubscriptions<
  T extends readonly CoSubscription<any, any>[],
>(subscriptions: T): (SubscriptionScope<CoValue> | null)[] {
  const contextManager = useJazzContextManager();
  const agent = useAgent();

  const callerStack = React.useRef<Error | undefined>(undefined);
  if (!callerStack.current) {
    callerStack.current = captureStack();
  }

  const createSubscriptionEntry = (
    sub: CoSubscription<any, any>,
  ): SubscriptionEntry => {
    if (!sub.id) {
      return {
        subscription: null,
        schema: sub.schema,
        id: sub.id,
        resolve: sub.resolve,
      };
    }

    const resolve = getResolveQuery(sub.schema, sub.resolve);
    const node = contextManager.getCurrentValue()!.node;
    const cache = contextManager.getSubscriptionScopeCache();

    const subscription = cache.getOrCreate(
      node,
      sub.schema,
      sub.id,
      resolve,
      false,
      false,
      undefined, // no branch support for now
    );

    if (callerStack.current) {
      subscription.callerStack = callerStack.current;
    }

    return {
      subscription,
      schema: sub.schema,
      id: sub.id,
      resolve: sub.resolve,
    };
  };

  const createAllSubscriptions = (): SubscriptionsState => {
    return {
      entries: subscriptions.map(createSubscriptionEntry),
      contextManager,
      agent,
    };
  };

  const stateRef = React.useRef<SubscriptionsState | null>(null);

  if (!stateRef.current) {
    stateRef.current = createAllSubscriptions();
  }

  let state = stateRef.current;

  // Check if we need to update due to context/agent changes
  const contextChanged =
    state.contextManager !== contextManager || state.agent !== agent;

  // Check if any subscription entry changed
  const entriesChanged =
    state.entries.length !== subscriptions.length ||
    subscriptions.some((sub, index) => {
      const entry = state.entries[index];
      if (!entry) return true;
      return (
        entry.schema !== sub.schema ||
        entry.id !== sub.id ||
        entry.resolve !== sub.resolve
      );
    });

  if (contextChanged || entriesChanged) {
    stateRef.current = createAllSubscriptions();
    state = stateRef.current;
  }

  return useMemo(
    () => state.entries.map((entry) => entry.subscription),
    [state.entries],
  );
}

/**
 * A promise with status tracking for the `use()` hook.
 */
type PromiseWithStatus<T> = Promise<T> & {
  status?: "pending" | "fulfilled" | "rejected";
  value?: T;
  reason?: unknown;
};

/**
 * Creates a resolved promise with the correct status for immediate use.
 */
function resolvedPromise<T>(value: T): PromiseWithStatus<T> {
  const promise = Promise.resolve(value) as PromiseWithStatus<T>;
  promise.status = "fulfilled";
  promise.value = value;
  return promise;
}

/**
 * Internal hook that creates a combined suspense promise from multiple subscriptions.
 *
 * - Creates a Promise.all from individual getCachedPromise() calls
 * - Returns Promise.resolve(null) for null subscriptions (undefined/null IDs)
 * - Suspends via the use() hook until all values are loaded
 *
 * @param subscriptions - Array of SubscriptionScope instances (or null for skipped entries)
 */
export function useMultiCoStateSuspense(
  subscriptions: (SubscriptionScope<CoValue> | null)[],
): void {
  // Create a stable key based on subscriptions to memoize the combined promise
  const subscriptionIds = subscriptions
    .map((sub) => sub?.id ?? "null")
    .join(",");

  const combinedPromise = useMemo(() => {
    const promises = subscriptions.map((sub) => {
      if (!sub) {
        // For null subscriptions (undefined/null IDs), resolve immediately with null
        return resolvedPromise(null);
      }
      return sub.getCachedPromise();
    });

    return Promise.all(promises);
  }, [subscriptionIds]);

  // Suspend until all promises are resolved
  use(combinedPromise);
}

/**
 * Internal hook that uses useSyncExternalStore to subscribe to multiple SubscriptionScopes.
 *
 * - Creates a combined subscribe function that subscribes to all scopes
 * - Returns an array of current values from each scope
 * - Maintains stable references for unchanged values
 *
 * @param subscriptions - Array of SubscriptionScope instances (or null for skipped entries)
 * @returns Array of loaded CoValues (or null for skipped entries)
 */
export function useMultiCoStateStore(
  subscriptions: (SubscriptionScope<CoValue> | null)[],
): (CoValue | null)[] {
  // Create a stable key for memoization
  const subscriptionIds = subscriptions
    .map((sub) => sub?.id ?? "null")
    .join(",");

  // Cache for the snapshot to avoid infinite loops
  const cachedSnapshotRef = useRef<(CoValue | null)[]>([]);

  // Combined subscribe function that subscribes to all scopes
  const subscribe = useCallback(
    (callback: () => void) => {
      const unsubscribes = subscriptions.map((sub) => {
        if (!sub) {
          return () => {};
        }
        return sub.subscribe(callback);
      });

      // Return combined unsubscribe function
      return () => {
        unsubscribes.forEach((unsub) => unsub());
      };
    },
    [subscriptionIds],
  );

  // Get current values from all subscriptions, with caching to prevent infinite loops
  const getSnapshot = useCallback(() => {
    const newValues = subscriptions.map((sub) => {
      if (!sub) {
        return null;
      }
      const value = sub.getCurrentValue();
      if (!value.$isLoaded) {
        throw new Error("CoValue must be loaded in a suspense context");
      }
      return value;
    });

    // Check if values have changed by comparing each element
    const cached = cachedSnapshotRef.current;
    const hasChanged =
      cached.length !== newValues.length ||
      newValues.some((value, index) => value !== cached[index]);

    if (hasChanged) {
      cachedSnapshotRef.current = newValues;
    }

    return cachedSnapshotRef.current;
  }, [subscriptionIds]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

/**
 * Subscribe to multiple CoValues with unified Suspense handling.
 *
 * This hook accepts an array of subscription definitions and returns a tuple of loaded values,
 * suspending until all values are available. It enables batched subscriptions with a single
 * Suspense boundary.
 *
 * @param subscriptions - Array of subscription objects with `schema`, `id`, and optional `resolve`
 * @returns A tuple of loaded CoValues in the same order as the input subscriptions
 *
 * @example
 * ```typescript
 * const [project, task] = useSuspenseMultiCoState([
 *   { schema: Project, id: projectId },
 *   { schema: Task, id: taskId, resolve: { assignee: true } },
 * ] as const);
 * ```
 *
 * @remarks
 * - Use `as const` for proper tuple type inference
 * - Entries with `undefined` or `null` IDs return `null` without suspending
 * - All valid entries suspend together until loaded
 */
export function useSuspenseMultiCoState<
  T extends readonly CoSubscription<any, any>[],
>(subscriptions: T): UseSuspenseMultiCoStateResult<T> {
  // Step 1: Create/get subscriptions for each entry
  const subscriptionScopes = useMultiCoStateSubscriptions(subscriptions);

  // Step 2: Suspend until all subscriptions are loaded
  useMultiCoStateSuspense(subscriptionScopes);

  // Step 3: Get current values via useSyncExternalStore
  const values = useMultiCoStateStore(subscriptionScopes);

  return values as UseSuspenseMultiCoStateResult<T>;
}

/**
 * Internal hook that uses useSyncExternalStore to subscribe to multiple SubscriptionScopes.
 * Returns MaybeLoaded values instead of throwing when values aren't loaded.
 *
 * @param subscriptions - Array of SubscriptionScope instances (or null for skipped entries)
 * @returns Array of MaybeLoaded CoValues (or null for skipped entries)
 */
function useMultiCoStateStoreMaybeLoaded(
  subscriptions: (SubscriptionScope<CoValue> | null)[],
): (MaybeLoaded<CoValue> | null)[] {
  // Create a stable key for memoization
  const subscriptionIds = subscriptions
    .map((sub) => sub?.id ?? "null")
    .join(",");

  // Cache for the snapshot to avoid infinite loops
  const cachedSnapshotRef = useRef<(MaybeLoaded<CoValue> | null)[]>([]);

  // Combined subscribe function that subscribes to all scopes
  const subscribe = useCallback(
    (callback: () => void) => {
      const unsubscribes = subscriptions.map((sub) => {
        if (!sub) {
          return () => {};
        }
        return sub.subscribe(callback);
      });

      // Return combined unsubscribe function
      return () => {
        unsubscribes.forEach((unsub) => unsub());
      };
    },
    [subscriptionIds],
  );

  // Get current values from all subscriptions (MaybeLoaded, no throwing)
  const getSnapshot = useCallback(() => {
    const newValues = subscriptions.map((sub) => {
      if (!sub) {
        return null;
      }
      return sub.getCurrentValue();
    });

    // Check if values have changed by comparing each element
    const cached = cachedSnapshotRef.current;
    const hasChanged =
      cached.length !== newValues.length ||
      newValues.some((value, index) => value !== cached[index]);

    if (hasChanged) {
      cachedSnapshotRef.current = newValues;
    }

    return cachedSnapshotRef.current;
  }, [subscriptionIds]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

/**
 * Subscribe to multiple CoValues without Suspense.
 *
 * This hook accepts an array of subscription definitions and returns a tuple of MaybeLoaded values.
 * Unlike `useSuspenseMultiCoState`, this hook does not suspend and returns loading/unavailable
 * states that can be checked via the `$isLoaded` property.
 *
 * @param subscriptions - Array of subscription objects with `schema`, `id`, and optional `resolve`
 * @returns A tuple of MaybeLoaded CoValues in the same order as the input subscriptions
 *
 * @example
 * ```typescript
 * const [project, task] = useMultiCoState([
 *   { schema: Project, id: projectId },
 *   { schema: Task, id: taskId, resolve: { assignee: true } },
 * ] as const);
 *
 * if (!project?.$isLoaded || !task?.$isLoaded) {
 *   return <Loading />;
 * }
 * ```
 *
 * @remarks
 * - Use `as const` for proper tuple type inference
 * - Entries with `undefined` or `null` IDs return `null`
 * - Check `$isLoaded` on each value to determine if it's ready
 */
export function useMultiCoState<T extends readonly CoSubscription<any, any>[]>(
  subscriptions: T,
): UseMultiCoStateResult<T> {
  // Step 1: Create/get subscriptions for each entry
  const subscriptionScopes = useMultiCoStateSubscriptions(subscriptions);

  // Step 2: Get current values via useSyncExternalStore (no suspending)
  const values = useMultiCoStateStoreMaybeLoaded(subscriptionScopes);

  return values as UseMultiCoStateResult<T>;
}

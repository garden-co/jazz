import React, {
  useCallback,
  useMemo,
  useRef,
  useSyncExternalStore,
} from "react";
import {
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
 * Tracked state for the entire subscriptions array.
 */
interface SubscriptionsState {
  subscriptions: SubscriptionScope<CoValue>[];
  schema: CoValueClassOrSchema;
  ids: readonly string[];
  resolve: ResolveQuery<any>;
  contextManager: ReturnType<typeof useJazzContextManager>;
  agent: AnonymousJazzAgent | Loaded<any, true>;
}

/**
 * Internal hook that manages an array of SubscriptionScope instances.
 *
 * - Uses a ref to track subscriptions by index
 * - Detects changes by comparing schema/ids/resolve
 * - Creates new subscriptions via SubscriptionScopeCache.getOrCreate()
 */
function useCoValueSubscriptions(
  schema: CoValueClassOrSchema,
  ids: readonly string[],
  resolve: ResolveQuery<any>,
): SubscriptionScope<CoValue>[] {
  const contextManager = useJazzContextManager();
  const agent = useAgent();

  const callerStack = React.useRef<Error | undefined>(undefined);
  if (!callerStack.current) {
    callerStack.current = captureStack();
  }

  const createAllSubscriptions = (): SubscriptionsState => {
    const node = contextManager.getCurrentValue()!.node;
    const cache = contextManager.getSubscriptionScopeCache();

    const subscriptions = ids.map((id) => {
      const subscription = cache.getOrCreate(
        node,
        schema,
        id,
        resolve,
        false,
        false,
        undefined, // no branch support for now
      );

      if (callerStack.current) {
        subscription.callerStack = callerStack.current;
      }

      return subscription;
    });

    return {
      subscriptions,
      schema,
      ids,
      resolve,
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

  // Check if schema/ids/resolve changed
  const paramsChanged =
    state.schema !== schema ||
    state.resolve !== resolve ||
    state.subscriptions.length !== ids.length ||
    state.subscriptions.some(
      (subscription, index) => subscription.id !== ids[index],
    );

  if (contextChanged || paramsChanged) {
    stateRef.current = createAllSubscriptions();
    state = stateRef.current;
  }

  return useMemo(() => state.subscriptions, [state.subscriptions]);
}

/**
 * Internal hook that suspends until all values are loaded.
 *
 * - Creates a Promise.all from individual getCachedPromise() calls
 * - Suspends via the use() hook until all values are loaded
 */
function useSuspendUntilLoaded(
  subscriptions: SubscriptionScope<CoValue>[],
): void {
  const subscriptionIds = subscriptions.map((sub) => sub.id).join(",");

  const combinedPromise = useMemo(() => {
    const promises = subscriptions.map((sub) => sub.getCachedPromise());
    return Promise.all(promises);
  }, [subscriptionIds]);

  use(combinedPromise);
}

/**
 * Internal hook that uses useSyncExternalStore to subscribe to multiple SubscriptionScopes.
 *
 * - Creates a combined subscribe function that subscribes to all scopes
 * - Returns an array of current values from each scope
 * - Maintains stable references for unchanged values
 *
 * @param subscriptions - Array of SubscriptionScope instances
 * @returns Array of loaded CoValues
 */
function useSubscriptionsSelector<T extends CoValue[] | MaybeLoaded<CoValue>[]>(
  subscriptions: SubscriptionScope<CoValue>[],
): T {
  // Create a stable key for memoization
  const subscriptionIds = subscriptions.map((sub) => sub.id).join(",");

  // Combined subscribe function that subscribes to all scopes
  const subscribe = useCallback(
    (callback: () => void) => {
      const unsubscribes = subscriptions.map((sub) => sub.subscribe(callback));
      return () => {
        unsubscribes.forEach((unsub) => unsub());
      };
    },
    [subscriptionIds],
  );

  // Cache current values to avoid infinite loops
  const cachedCurrentValuesRef = useRef<T>([] as unknown as T);
  const getCurrentValues = useCallback(() => {
    const newValues = subscriptions.map((sub) => sub.getCurrentValue());

    // Check if values have changed by comparing each element
    const cached = cachedCurrentValuesRef.current;
    const hasChanged =
      cached.length !== newValues.length ||
      newValues.some((value, index) => value !== cached[index]);

    if (hasChanged) {
      cachedCurrentValuesRef.current = newValues as T;
    }

    return cachedCurrentValuesRef.current;
  }, [subscriptionIds]);

  return useSyncExternalStore(subscribe, getCurrentValues, getCurrentValues);
}

/**
 * Subscribe to multiple CoValues with unified Suspense handling.
 *
 * This hook accepts a schema, resolve query, and a list of IDs, returning an array of loaded values,
 * suspending until all values are available. It enables batched subscriptions with a single
 * Suspense boundary.
 *
 * @param Schema - The CoValue schema or class constructor (same for all IDs)
 * @param ids - Array of CoValue IDs to subscribe to (must all be strings)
 * @param options - Optional configuration, including resolve query (same for all IDs)
 * @returns An array of loaded CoValues in the same order as the input IDs
 *
 * @example
 * ```typescript
 * const [project1, project2] = useSuspenseCoStates(
 *   ProjectSchema,
 *   [projectId1, projectId2],
 *   { resolve: { assignee: true } }
 * );
 * ```
 *
 * @remarks
 * - All IDs use the same schema and resolve query
 * - All entries suspend together until loaded
 */
export function useSuspenseCoStates<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(
  Schema: S,
  ids: readonly string[],
  options?: {
    /** Resolve query to specify which nested CoValues to load (same for all IDs) */
    resolve?: ResolveQueryStrict<S, R>;
  },
): Loaded<S, R>[] {
  const resolve = getResolveQuery(Schema, options?.resolve);
  const subscriptionScopes = useCoValueSubscriptions(Schema, ids, resolve);
  useSuspendUntilLoaded(subscriptionScopes);
  return useSubscriptionsSelector<Loaded<S, R>[]>(subscriptionScopes);
}

/**
 * Subscribe to multiple CoValues without Suspense.
 *
 * This hook accepts a schema, resolve query, and a list of IDs, returning an array of MaybeLoaded values.
 * Unlike `useSuspenseCoStates`, this hook does not suspend and returns loading/unavailable
 * states that can be checked via the `$isLoaded` property.
 *
 * @param Schema - The CoValue schema or class constructor
 * @param ids - Array of CoValue IDs to subscribe to (must all be strings)
 * @param options - Optional configuration, including resolve query
 * @returns An array of MaybeLoaded CoValues in the same order as the input IDs
 *
 * @example
 * ```typescript
 * const [project1, project2] = useCoStates(
 *   ProjectSchema,
 *   [projectId1, projectId2],
 *   { resolve: { assignee: true } }
 * );
 *
 * if (!project1.$isLoaded || !project2.$isLoaded) {
 *   return <Loading />;
 * }
 * ```
 *
 * @remarks
 * - All IDs use the same schema and resolve query
 * - Check `$isLoaded` on each value to determine if it's ready
 */
export function useCoStates<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(
  Schema: S,
  ids: readonly string[],
  options?: {
    /** Resolve query to specify which nested CoValues to load (same for all IDs) */
    resolve?: ResolveQueryStrict<S, R>;
  },
): MaybeLoaded<Loaded<S, R>>[] {
  const resolve = getResolveQuery(Schema, options?.resolve);
  const subscriptionScopes = useCoValueSubscriptions(Schema, ids, resolve);
  return useSubscriptionsSelector<MaybeLoaded<Loaded<S, R>>[]>(
    subscriptionScopes,
  );
}

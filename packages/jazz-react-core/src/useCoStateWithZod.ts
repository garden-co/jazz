import { AnonymousJazzAgent, SchemaV2 } from "jazz-tools";
import type { Account } from "jazz-tools";
import type {
  CoValueSchema,
  ID,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  Unloaded,
} from "jazz-tools/dist/zodSchema/schema.js";
import { useCallback, useRef, useState, useSyncExternalStore } from "react";
import { useJazzContextManager } from "./hooks.js";
import {
  getCurrentAccountFromContextManager,
  subscribeToContextManager,
} from "./utils.js";

export function createCoValueObservable<
  V extends CoValueSchema,
  const R extends ResolveQuery<V>,
>() {
  let currentValue: Loaded<V, R> | Unloaded<V>;

  function subscribe(
    cls: V,
    id: ID<V>,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      resolve?: ResolveQueryStrict<V, R>;
      syncResolution?: boolean;
    },
    listener: () => void,
  ) {
    currentValue = SchemaV2.getUnloadedState(cls, id);

    const unsubscribe = SchemaV2.subscribeToCoValue(
      cls,
      id,
      {
        loadAs: options.loadAs,
        resolve: options.resolve,
      },
      (value) => {
        currentValue = value;
        listener();
      },
    );

    return () => {
      unsubscribe();
    };
  }

  const observable = {
    getCurrentValue: (): Loaded<V, R> | Unloaded<V> => currentValue,
    subscribe,
  };

  return observable;
}

function useCoValueObservable<
  V extends CoValueSchema,
  const R extends ResolveQuery<V>,
>() {
  const [initialValue] = useState(() => createCoValueObservable<V, R>());
  const ref = useRef(initialValue);

  return {
    getCurrentValue() {
      return ref.current.getCurrentValue();
    },
    getCurrentObservable() {
      return ref.current;
    },
    reset() {
      ref.current = createCoValueObservable<V, R>();
    },
  };
}

export function useCoStateWithZod<
  V extends CoValueSchema,
  const R extends ResolveQuery<V> = true,
>(
  Schema: V,
  id: ID<V> | undefined,
  options?: { resolve?: ResolveQueryStrict<V, R> },
): Loaded<V, R> | Unloaded<V> {
  const contextManager = useJazzContextManager();

  const observable = useCoValueObservable<V, R>();

  const value = useSyncExternalStore<Loaded<V, R> | Unloaded<V>>(
    useCallback(
      (callback) => {
        observable.reset(); // TODO: Cover this with a test

        if (!id) {
          // What should we return here?
          return () => {};
        }

        // We subscribe to the context manager to react to the account updates
        // faster than the useSyncExternalStore callback update to keep the isAuthenticated state
        // up to date with the data when logging in and out.
        return subscribeToContextManager(contextManager, () => {
          const agent = getCurrentAccountFromContextManager(contextManager);
          observable.reset();

          return observable.getCurrentObservable().subscribe(
            Schema,
            id,
            {
              loadAs: agent,
              resolve: options?.resolve,
            },
            callback,
          );
        });
      },
      [Schema, id, contextManager],
    ),
    () => observable.getCurrentValue(),
    () => observable.getCurrentValue(),
  );

  return value;
}

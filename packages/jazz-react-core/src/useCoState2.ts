import {
  AnonymousJazzAgent,
  ID,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SchemaV2,
} from "jazz-tools";
import type { Account } from "jazz-tools";
import type {
  CoMapSchema,
  CoMapSchemaClass,
} from "jazz-tools/dist/schema/coMap.js";
import { useCallback, useRef, useState, useSyncExternalStore } from "react";
import { useJazzContextManager } from "./hooks.js";
import {
  getCurrentAccountFromContextManager,
  subscribeToContextManager,
} from "./utils.js";

export function createCoValueObservable<
  V extends CoMapSchema,
  const R extends RefsToResolve<V>,
>() {
  let currentValue: Resolved<V, R> | undefined | null = undefined;
  let subscriberCount = 0;

  function subscribe(
    cls: CoMapSchemaClass<V>,
    id: ID<CoMapSchema>,
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

    const unsubscribe = SchemaV2.subscribeToCoValue(
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

function useCoValueObservable<
  V extends CoMapSchema,
  const R extends RefsToResolve<V>,
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

export function useCoState2<
  V extends CoMapSchema,
  const R extends RefsToResolve<V> = true,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Schema: CoMapSchemaClass<V>,
  id: ID<CoMapSchema> | undefined,
  options?: { resolve?: RefsToResolveStrict<V, R> },
): Resolved<V, R> | undefined | null {
  const contextManager = useJazzContextManager();

  const observable = useCoValueObservable<V, R>();

  const value = useSyncExternalStore<Resolved<V, R> | undefined | null>(
    useCallback(
      (callback) => {
        if (!id) return () => {};

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
              onUnauthorized: callback,
              onUnavailable: callback,
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

import { type Account } from "../coValues/account.js";
import {
  AnonymousJazzAgent,
  CoValue,
  ID,
  RefEncoded,
  SubscriptionScope,
  LoadedAndRequired,
  accessChildById,
  CoValueLoadingState,
  getSubscriptionScope,
  isRefEncoded,
  createUnloadedCoValue,
  MaybeLoaded,
  CoreCoValueSchema,
  Loaded,
  CoreAccountSchema,
} from "../internal.js";

export class Ref<out S extends CoreCoValueSchema> {
  constructor(
    readonly id: ID<S>,
    readonly controlledAccount:
      | Loaded<CoreAccountSchema, true>
      | AnonymousJazzAgent,
    readonly schema: RefEncoded<S>,
    readonly parent: CoValue,
  ) {
    if (!isRefEncoded(schema)) {
      throw new Error("Ref must be constructed with a ref schema");
    }
  }

  async load(): Promise<MaybeLoaded<S>> {
    const subscriptionScope = getSubscriptionScope(this.parent);

    let node: SubscriptionScope<S> | undefined;

    /**
     * If the parent subscription scope is closed, we can't use it
     * to subscribe to the child id, so we create a detached subscription scope
     * that is going to be destroyed immediately after the load
     */
    if (subscriptionScope.closed) {
      node = new SubscriptionScope<S>(
        subscriptionScope.node,
        true,
        this.id,
        this.schema,
        subscriptionScope.skipRetry,
        subscriptionScope.bestEffortResolution,
        subscriptionScope.unstable_branch,
      );
    } else {
      subscriptionScope.subscribeToId(this.id, this.schema);

      node = subscriptionScope.childNodes.get(this.id) as
        | SubscriptionScope<S>
        | undefined;
    }

    if (!node) {
      return createUnloadedCoValue(this.id, CoValueLoadingState.UNAVAILABLE);
    }

    const value = node.getCurrentValue();

    if (value.$isLoaded) {
      return value;
    } else {
      return new Promise((resolve) => {
        const unsubscribe = node.subscribe(() => {
          const currentValue = node.getCurrentValue();

          if (currentValue.$jazz.loadingState !== CoValueLoadingState.LOADING) {
            unsubscribe();
            resolve(currentValue);
          }

          if (subscriptionScope.closed) {
            node.destroy();
          }
        });
      });
    }
  }

  get value(): MaybeLoaded<S> {
    return accessChildById(this.parent, this.id, this.schema);
  }
}

export function makeRefs<Keys extends string | number>(
  parent: CoValue,
  getIdForKey: (key: Keys) => ID<CoValue> | undefined,
  getKeysWithIds: () => Keys[],
  controlledAccount: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent,
  refSchemaForKey: (key: Keys) => RefEncoded<CoreCoValueSchema>,
): { [K in Keys]: Ref<CoreCoValueSchema> } & {
  [Symbol.iterator]: () => IterableIterator<Ref<CoreCoValueSchema>>;
  length: number;
} {
  const refs = {} as { [K in Keys]: Ref<CoreCoValueSchema> } & {
    [Symbol.iterator]: () => IterableIterator<Ref<CoreCoValueSchema>>;
    length: number;
  };
  return new Proxy(refs, {
    get(_target, key) {
      if (key === Symbol.iterator) {
        return function* () {
          for (const key of getKeysWithIds()) {
            yield new Ref(
              getIdForKey(key)!,
              controlledAccount,
              refSchemaForKey(key),
              parent,
            );
          }
        };
      }
      if (typeof key === "symbol") return undefined;
      if (key === "length") {
        return getKeysWithIds().length;
      }
      const id = getIdForKey(key as Keys);
      if (!id) return undefined;
      return new Ref(
        id as ID<CoValue>,
        controlledAccount,
        refSchemaForKey(key as Keys),
        parent,
      );
    },
    ownKeys() {
      return getKeysWithIds().map((key) => key.toString());
    },
    getOwnPropertyDescriptor(target, key) {
      const id = getIdForKey(key as Keys);
      if (id) {
        return {
          enumerable: true,
          configurable: true,
          writable: true,
        };
      } else {
        return Reflect.getOwnPropertyDescriptor(target, key);
      }
    },
  });
}

export type RefIfCoValue<S> = S extends CoreCoValueSchema ? Ref<S> : never;

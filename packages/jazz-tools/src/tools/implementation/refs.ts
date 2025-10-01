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
} from "../internal.js";

export class Ref<out V extends CoValue> {
  constructor(
    readonly id: ID<V>,
    readonly controlledAccount: Account | AnonymousJazzAgent,
    readonly schema: RefEncoded<V>,
    readonly parent: CoValue,
  ) {
    if (!isRefEncoded(schema)) {
      throw new Error("Ref must be constructed with a ref schema");
    }
  }

  async load(): Promise<MaybeLoaded<V>> {
    const subscriptionScope = getSubscriptionScope(this.parent);

    let node: SubscriptionScope<CoValue> | undefined;

    /**
     * If the parent subscription scope is closed, we can't use it
     * to subscribe to the child id, so we create a detached subscription scope
     * that is going to be destroyed immediately after the load
     */
    if (subscriptionScope.closed) {
      node = new SubscriptionScope<CoValue>(
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

      node = subscriptionScope.childNodes.get(this.id);
    }

    if (!node) {
      return createUnloadedCoValue(this.id, CoValueLoadingState.UNLOADED);
    }

    const value = node.value;

    if (value?.type === CoValueLoadingState.LOADED) {
      return value.value as V;
    } else {
      return new Promise((resolve) => {
        const unsubscribe = node.subscribe((value) => {
          if (value?.type === CoValueLoadingState.LOADED) {
            unsubscribe();
            resolve(value.value as V);
          } else if (value?.type === CoValueLoadingState.UNAVAILABLE) {
            unsubscribe();
            resolve(
              createUnloadedCoValue(this.id, CoValueLoadingState.UNAVAILABLE),
            );
          } else if (value?.type === CoValueLoadingState.UNAUTHORIZED) {
            unsubscribe();
            resolve(
              createUnloadedCoValue(this.id, CoValueLoadingState.UNAUTHORIZED),
            );
          }

          if (subscriptionScope.closed) {
            node.destroy();
          }
        });
      });
    }
  }

  get value(): V | null | undefined {
    return accessChildById(this.parent, this.id, this.schema);
  }
}

export function makeRefs<Keys extends string | number>(
  parent: CoValue,
  getIdForKey: (key: Keys) => ID<CoValue> | undefined,
  getKeysWithIds: () => Keys[],
  controlledAccount: Account | AnonymousJazzAgent,
  refSchemaForKey: (key: Keys) => RefEncoded<CoValue>,
): { [K in Keys]: Ref<CoValue> } & {
  [Symbol.iterator]: () => IterableIterator<Ref<CoValue>>;
  length: number;
} {
  const refs = {} as { [K in Keys]: Ref<CoValue> } & {
    [Symbol.iterator]: () => IterableIterator<Ref<CoValue>>;
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

export type RefIfCoValue<V> = LoadedAndRequired<V> extends CoValue
  ? Ref<LoadedAndRequired<V>>
  : never;

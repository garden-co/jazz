import { type Account } from "../coValues/account.js";
import type {
  AnonymousJazzAgent,
  CoValue,
  ID,
  RefEncoded,
} from "../internal.js";
import {
  accessChildById,
  getSubscriptionScope,
  isRefEncoded,
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

  async load(): Promise<V | null> {
    const subscriptionScope = getSubscriptionScope(this.parent);

    subscriptionScope.subscribeToId(this.id, this.schema);

    const node = subscriptionScope.childNodes.get(this.id);

    if (!node) {
      return null;
    }

    const value = node.value;

    if (value?.type === "loaded") {
      return value.value as V;
    } else {
      return new Promise((resolve) => {
        const unsubscribe = node.subscribe((value) => {
          if (value?.type === "loaded") {
            unsubscribe();
            resolve(value.value as V);
          } else if (value?.type === "unavailable") {
            unsubscribe();
            resolve(null);
          } else if (value?.type === "unauthorized") {
            unsubscribe();
            resolve(null);
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

export type RefIfCoValue<V> = NonNullable<V> extends CoValue
  ? Ref<NonNullable<V>>
  : never;

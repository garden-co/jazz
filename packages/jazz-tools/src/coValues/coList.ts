import type { JsonValue, RawCoList } from "cojson";
import { RawAccount } from "cojson";
import { calcPatch } from "fast-myers-diff";
import type {
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  ID,
  RefEncoded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  SchemaFor,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  UnCo,
} from "../internal.js";
import {
  AnonymousJazzAgent,
  ItemsSym,
  Ref,
  SchemaInit,
  co,
  ensureCoValueLoaded,
  inspect,
  isRefEncoded,
  loadCoValueWithoutMe,
  makeRefs,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
  subscriptionsScopes,
} from "../internal.js";
import { coValuesCache } from "../lib/cache.js";
import { RegisteredAccount } from "../types.js";
import { type Account } from "./account.js";
import { type Group } from "./group.js";
import { RegisteredSchemas } from "./registeredSchemas.js";

/**
 * CoLists are collaborative versions of plain arrays.
 *
 * @categoryDescription Content
 * You can access items on a `CoList` as if they were normal items on a plain array, using `[]` notation, etc.
 *
 * Since `CoList` is a subclass of `Array`, you can use all the normal array methods like `push`, `pop`, `splice`, etc.
 *
 * ```ts
 * colorList[0];
 * colorList[3] = "yellow";
 * colorList.push("Kawazaki Green");
 * colorList.splice(1, 1);
 * ```
 *
 * @category CoValues
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class CoList<Item = any> extends Array<Item> implements CoValue {
  /**
   * Declare a `CoList` by subclassing `CoList.Of(...)` and passing the item schema using `co`.
   *
   * @example
   * ```ts
   * class ColorList extends CoList.Of(
   *   co.string
   * ) {}
   * class AnimalList extends CoList.Of(
   *   co.ref(Animal)
   * ) {}
   * ```
   *
   * @category Declaration
   */
  static Of<Item>(item: Item): typeof CoList<Item> {
    // TODO: cache superclass for item class
    return class CoListOf extends CoList<Item> {
      [co.items] = item;
    };
  }

  /**
   * @ignore
   * @deprecated Use UPPERCASE `CoList.Of` instead! */
  static of(..._args: never): never {
    throw new Error("Can't use Array.of with CoLists");
  }

  /**
   * The ID of this `CoList`
   * @category Content */
  declare id: ID<this>;
  /** @category Type Helpers */
  declare _type: "CoList";
  static {
    this.prototype._type = "CoList";
  }
  /** @category Internals */
  declare _raw: RawCoList;
  /** @category Internals */
  declare _instanceID: string;

  /** @internal This is only a marker type and doesn't exist at runtime */
  [ItemsSym]!: Item;
  /** @internal */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;
  /** @internal */
  get _schema(): {
    [ItemsSym]: SchemaFor<Item>;
  } {
    return (this.constructor as typeof CoList)._schema;
  }

  /** @category Collaboration */
  get _owner(): Account | Group {
    return this._raw.group instanceof RawAccount
      ? RegisteredSchemas["Account"].fromRaw(this._raw.group)
      : RegisteredSchemas["Group"].fromRaw(this._raw.group);
  }

  /**
   * If a `CoList`'s items are a `co.ref(...)`, you can use `coList._refs[i]` to access
   * the `Ref` instead of the potentially loaded/null value.
   *
   * This allows you to always get the ID or load the value manually.
   *
   * @example
   * ```ts
   * animals._refs[0].id; // => ID<Animal>
   * animals._refs[0].value;
   * // => Animal | null
   * const animal = await animals._refs[0].load();
   * ```
   *
   * @category Content
   **/
  get _refs(): {
    [idx: number]: Exclude<Item, null> extends CoValue
      ? Ref<UnCo<Exclude<Item, null>>>
      : never;
  } & {
    length: number;
    [Symbol.iterator](): IterableIterator<
      Exclude<Item, null> extends CoValue ? Ref<Exclude<Item, null>> : never
    >;
  } {
    return makeRefs<number>(
      (idx) => this._raw.get(idx) as unknown as ID<CoValue>,
      () => Array.from({ length: this._raw.entries().length }, (_, idx) => idx),
      this._loadedAs,
      (_idx) => this._schema[ItemsSym] as RefEncoded<CoValue>,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ) as any;
  }

  get _edits(): {
    [idx: number]: {
      value?: Item;
      ref?: Item extends CoValue ? Ref<Item> : never;
      by?: RegisteredAccount;
      madeAt: Date;
    };
  } {
    throw new Error("Not implemented");
  }

  get _loadedAs() {
    const rawAccount = this._raw.core.node.account;

    if (rawAccount instanceof RawAccount) {
      return coValuesCache.get(rawAccount, () =>
        RegisteredSchemas["Account"].fromRaw(rawAccount),
      );
    }

    return new AnonymousJazzAgent(this._raw.core.node);
  }

  static get [Symbol.species]() {
    return Array;
  }

  constructor(options: { fromRaw: RawCoList } | undefined) {
    super();

    Object.defineProperty(this, "_instanceID", {
      value: `instance-${Math.random().toString(36).slice(2)}`,
      enumerable: false,
    });

    if (options && "fromRaw" in options) {
      Object.defineProperties(this, {
        id: {
          value: options.fromRaw.id,
          enumerable: false,
        },
        _raw: { value: options.fromRaw, enumerable: false },
      });
    }

    return new Proxy(this, CoListProxyHandler as ProxyHandler<this>);
  }

  /**
   * Create a new CoList with the given initial values and owner.
   *
   * The owner (a Group or Account) determines access rights to the CoMap.
   *
   * The CoList will immediately be persisted and synced to connected peers.
   *
   * @example
   * ```ts
   * const colours = ColorList.create(
   *   ["red", "green", "blue"],
   *   { owner: me }
   * );
   * const animals = AnimalList.create(
   *   [cat, dog, fish],
   *   { owner: me }
   * );
   * ```
   *
   * @category Creation
   **/
  static create<L extends CoList>(
    this: CoValueClass<L>,
    items: UnCo<L[number]>[],
    options?: { owner: Account | Group } | Account | Group,
  ) {
    const { owner } = parseCoValueCreateOptions(options);
    const instance = new this({ init: items, owner });
    const raw = owner._raw.createList(
      toRawItems(items, instance._schema[ItemsSym]),
    );

    Object.defineProperties(instance, {
      id: {
        value: raw.id,
        enumerable: false,
      },
      _raw: { value: raw, enumerable: false },
    });

    return instance;
  }

  push(...items: Item[]): number {
    this._raw.appendItems(
      toRawItems(items, this._schema[ItemsSym]),
      undefined,
      "private",
    );

    return this._raw.entries().length;
  }

  unshift(...items: Item[]): number {
    for (const item of toRawItems(items as Item[], this._schema[ItemsSym])) {
      this._raw.prepend(item);
    }

    return this._raw.entries().length;
  }

  pop(): Item | undefined {
    const last = this[this.length - 1];

    this._raw.delete(this.length - 1);

    return last;
  }

  shift(): Item | undefined {
    const first = this[0];

    this._raw.delete(0);

    return first;
  }

  /**
   * Splice the `CoList` at a given index.
   *
   * @param start - The index to start the splice.
   * @param deleteCount - The number of items to delete.
   * @param items - The items to insert.
   */
  splice(start: number, deleteCount: number, ...items: Item[]): Item[] {
    const deleted = this.slice(start, start + deleteCount);

    for (
      let idxToDelete = start + deleteCount - 1;
      idxToDelete >= start;
      idxToDelete--
    ) {
      this._raw.delete(idxToDelete);
    }

    const rawItems = toRawItems(items as Item[], this._schema[ItemsSym]);

    // If there are no items to insert, return the deleted items
    if (rawItems.length === 0) {
      return deleted;
    }

    // Fast path for single item insertion
    if (rawItems.length === 1) {
      const item = rawItems[0];
      if (item === undefined) return deleted;
      if (start === 0) {
        this._raw.prepend(item);
      } else {
        this._raw.append(item, Math.max(start - 1, 0));
      }
      return deleted;
    }

    // Handle multiple items
    if (start === 0) {
      // Iterate in reverse order without creating a new array
      for (let i = rawItems.length - 1; i >= 0; i--) {
        const item = rawItems[i];
        if (item === undefined) continue;
        this._raw.prepend(item);
      }
    } else {
      let appendAfter = Math.max(start - 1, 0);
      for (const item of rawItems) {
        if (item === undefined) continue;
        this._raw.append(item, appendAfter);
        appendAfter++;
      }
    }

    return deleted;
  }

  /**
   * Modify the `CoList` to match another list, where the changes are managed internally.
   *
   * @param result - The resolved list of items.
   */
  applyDiff(result: Item[]) {
    const current = this._raw.asArray() as Item[];
    const comparator = isRefEncoded(this._schema[ItemsSym])
      ? (aIdx: number, bIdx: number) => {
          return (
            (current[aIdx] as CoValue)?.id === (result[bIdx] as CoValue)?.id
          );
        }
      : undefined;

    const patches = [...calcPatch(current, result, comparator)];
    for (const [from, to, insert] of patches.reverse()) {
      this.splice(from, to - from, ...insert);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, seenAbove?: ID<CoValue>[]): any[] {
    const itemDescriptor = this._schema[ItemsSym] as Schema;
    if (itemDescriptor === "json") {
      return this._raw.asArray();
    } else if ("encoded" in itemDescriptor) {
      return this._raw.asArray().map((e) => itemDescriptor.encoded.encode(e));
    } else if (isRefEncoded(itemDescriptor)) {
      return this.map((item, idx) =>
        seenAbove?.includes((item as CoValue)?.id)
          ? { _circular: (item as CoValue).id }
          : (item as unknown as CoValue)?.toJSON(idx + "", [
              ...(seenAbove || []),
              this.id,
            ]),
      );
    } else {
      return [];
    }
  }

  [inspect]() {
    return this.toJSON();
  }

  /** @category Internals */
  static fromRaw<V extends CoList>(
    this: CoValueClass<V> & typeof CoList,
    raw: RawCoList,
  ) {
    return new this({ fromRaw: raw });
  }

  /** @internal */
  static schema<V extends CoList>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this: { new (...args: any): V } & typeof CoList,
    def: { [ItemsSym]: V["_schema"][ItemsSym] },
  ) {
    this._schema ||= {};
    Object.assign(this._schema, def);
  }

  /**
   * Load a `CoList` with a given ID, as a given account.
   *
   * `depth` specifies if item CoValue references should be loaded as well before resolving.
   * The `DeeplyLoaded` return type guarantees that corresponding referenced CoValues are loaded to the specified depth.
   *
   * You can pass `[]` or for shallowly loading only this CoList, or `[itemDepth]` for recursively loading referenced CoValues.
   *
   * Check out the `load` methods on `CoMap`/`CoList`/`CoFeed`/`Group`/`Account` to see which depth structures are valid to nest.
   *
   * @example
   * ```ts
   * const animalsWithVets =
   *   await ListOfAnimals.load(
   *     "co_zdsMhHtfG6VNKt7RqPUPvUtN2Ax",
   *     me,
   *     [{ vet: {} }]
   *   );
   * ```
   *
   * @category Subscription & Loading
   */
  static load<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: CoValueClass<L>,
    id: ID<L>,
    options?: {
      resolve?: RefsToResolveStrict<L, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<L, R> | null> {
    return loadCoValueWithoutMe(this, id, options);
  }

  /**
   * Load and subscribe to a `CoList` with a given ID, as a given account.
   *
   * Automatically also subscribes to updates to all referenced/nested CoValues as soon as they are accessed in the listener.
   *
   * `depth` specifies if item CoValue references should be loaded as well before calling `listener` for the first time.
   * The `DeeplyLoaded` return type guarantees that corresponding referenced CoValues are loaded to the specified depth.
   *
   * You can pass `[]` or for shallowly loading only this CoList, or `[itemDepth]` for recursively loading referenced CoValues.
   *
   * Check out the `load` methods on `CoMap`/`CoList`/`CoFeed`/`Group`/`Account` to see which depth structures are valid to nest.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * Also see the `useCoState` hook to reactively subscribe to a CoValue in a React component.
   *
   * @example
   * ```ts
   * const unsub = ListOfAnimals.subscribe(
   *   "co_zdsMhHtfG6VNKt7RqPUPvUtN2Ax",
   *   me,
   *   { vet: {} },
   *   (animalsWithVets) => console.log(animalsWithVets)
   * );
   * ```
   *
   * @category Subscription & Loading
   */
  static subscribe<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: CoValueClass<L>,
    id: ID<L>,
    listener: (value: Resolved<L, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: CoValueClass<L>,
    id: ID<L>,
    options: SubscribeListenerOptions<L, R>,
    listener: (value: Resolved<L, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<L extends CoList, const R extends RefsToResolve<L>>(
    this: CoValueClass<L>,
    id: ID<L>,
    ...args: SubscribeRestArgs<L, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<L, R>(this, id, options, listener);
  }

  /**
   * Given an already loaded `CoList`, ensure that items are loaded to the specified depth.
   *
   * Works like `CoList.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<L extends CoList, const R extends RefsToResolve<L>>(
    this: L,
    options: { resolve: RefsToResolveStrict<L, R> },
  ): Promise<Resolved<L, R>> {
    return ensureCoValueLoaded(this, options);
  }

  /**
   * Given an already loaded `CoList`, subscribe to updates to the `CoList` and ensure that items are loaded to the specified depth.
   *
   * Works like `CoList.subscribe()`, but you don't need to pass the ID or the account to load as again.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * @category Subscription & Loading
   **/
  subscribe<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: L,
    listener: (value: Resolved<L, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: L,
    options: { resolve?: RefsToResolveStrict<L, R> },
    listener: (value: Resolved<L, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<L extends CoList, const R extends RefsToResolve<L>>(
    this: L,
    ...args: SubscribeRestArgs<L, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this, options, listener);
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

  /**
   * Wait for the `CoList` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this._raw.core.waitForSync(options);
  }
}

/**
 * Convert an array of items to a raw array of items.
 * @param items - The array of items to convert.
 * @param itemDescriptor - The descriptor of the items.
 * @returns The raw array of items.
 */
function toRawItems<Item>(items: Item[], itemDescriptor: Schema) {
  const rawItems =
    itemDescriptor === "json"
      ? (items as JsonValue[])
      : "encoded" in itemDescriptor
        ? items?.map((e) => itemDescriptor.encoded.encode(e))
        : isRefEncoded(itemDescriptor)
          ? items?.map((v) => {
              if (!v) return null;

              return (v as unknown as CoValue).id;
            })
          : (() => {
              throw new Error("Invalid element descriptor");
            })();
  return rawItems;
}

const CoListProxyHandler: ProxyHandler<CoList> = {
  get(target, key, receiver) {
    if (typeof key === "string" && !isNaN(+key)) {
      const itemDescriptor = target._schema[ItemsSym] as Schema;
      const rawValue = target._raw.get(Number(key));
      if (itemDescriptor === "json") {
        return rawValue;
      } else if ("encoded" in itemDescriptor) {
        return rawValue === undefined
          ? undefined
          : itemDescriptor.encoded.decode(rawValue);
      } else if (isRefEncoded(itemDescriptor)) {
        return rawValue === undefined
          ? undefined
          : new Ref(
              rawValue as unknown as ID<CoValue>,
              target._loadedAs,
              itemDescriptor,
            ).accessFrom(receiver, Number(key));
      }
    } else if (key === "length") {
      return target._raw.entries().length;
    } else {
      return Reflect.get(target, key, receiver);
    }
  },
  set(target, key, value, receiver) {
    if (key === ItemsSym && typeof value === "object" && SchemaInit in value) {
      (target.constructor as typeof CoList)._schema ||= {};
      (target.constructor as typeof CoList)._schema[ItemsSym] =
        value[SchemaInit];
      return true;
    }
    if (typeof key === "string" && !isNaN(+key)) {
      const itemDescriptor = target._schema[ItemsSym] as Schema;
      let rawValue;
      if (itemDescriptor === "json") {
        rawValue = value;
      } else if ("encoded" in itemDescriptor) {
        rawValue = itemDescriptor.encoded.encode(value);
      } else if (isRefEncoded(itemDescriptor)) {
        if (value === null) {
          if (itemDescriptor.optional) {
            rawValue = null;
          } else {
            throw new Error(`Cannot set required reference ${key} to null`);
          }
        } else if (value?.id) {
          rawValue = value.id;
        } else {
          throw new Error(
            `Cannot set reference ${key} to a non-CoValue. Got ${value}`,
          );
        }
      }
      target._raw.replace(Number(key), rawValue);
      return true;
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  defineProperty(target, key, descriptor) {
    if (
      descriptor.value &&
      key === ItemsSym &&
      typeof descriptor.value === "object" &&
      SchemaInit in descriptor.value
    ) {
      (target.constructor as typeof CoList)._schema ||= {};
      (target.constructor as typeof CoList)._schema[ItemsSym] =
        descriptor.value[SchemaInit];
      return true;
    } else {
      return Reflect.defineProperty(target, key, descriptor);
    }
  },
  has(target, key) {
    if (typeof key === "string" && !isNaN(+key)) {
      return Number(key) < target._raw.entries().length;
    } else {
      return Reflect.has(target, key);
    }
  },
};

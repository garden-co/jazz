import type { CoValueUniqueness, JsonValue, RawCoID, RawCoList } from "cojson";
import { cojsonInternals } from "cojson";
import { calcPatch } from "fast-myers-diff";
import {
  Account,
  CoFieldInit,
  CoValue,
  CoValueClass,
  CoValueJazzApi,
  getCoValueOwner,
  Group,
  ID,
  AsLoaded,
  Settled,
  unstable_mergeBranch,
  RefEncoded,
  FieldDescriptor,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  BranchDefinition,
  CoValueLoadingState,
  AnonymousJazzAgent,
  ItemsMarker,
  Ref,
  accessChildByKey,
  activeAccountContext,
  ensureCoValueLoaded,
  inspect,
  instantiateRefEncodedWithInit,
  isRefEncoded,
  makeRefs,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
  CoreCoListSchema,
  schemaFieldToFieldDescriptor,
  SchemaField,
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
  CoreCoValueSchema,
  MaybeLoaded,
  PrimitiveOrInaccessible,
  CoreAccountSchema,
} from "../internal.js";

/**
 * CoLists are collaborative versions of plain arrays.
 *
 * @categoryDescription Content
 * You can access items on a `CoList` as if they were normal items on a plain array, using `[]` notation, etc.
 *
 * All readonly array methods are available on `CoList`. You can also use the `.$jazz` API to mutate the CoList.
 *
 * ```ts
 * const colorList = ColorList.create(["red", "green", "blue"]);
 * ```
 *
 * ```ts
 * colorList[0];
 * colorList.$jazz.set(3, "yellow");
 * colorList.$jazz.push("Kawazaki Green");
 * colorList.$jazz.splice(1, 1);
 * ```
 *
 * @category CoValues
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class CoList<S extends CoreCoListSchema>
  extends Array<PrimitiveOrInaccessible<S["element"]>>
  implements ReadonlyArray<PrimitiveOrInaccessible<S["element"]>>, CoValue
{
  declare $jazz: CoListJazzApi<this, S>;
  declare $isLoaded: true;

  /**
   * @ignore
   * @deprecated Can't use Array.of with CoLists */
  static of(..._args: never): never {
    throw new Error("Can't use Array.of with CoLists");
  }

  /** @category Type Helpers */
  declare [TypeSym]: "CoList";
  static {
    this.prototype[TypeSym] = "CoList";
  }

  static get [Symbol.species]() {
    return Array;
  }

  constructor(raw: RawCoList, sourceSchema: S) {
    super();

    const proxy = new Proxy(
      this,
      CoListProxyHandler as unknown as ProxyHandler<this>,
    ) as this;

    Object.defineProperties(this, {
      $jazz: {
        value: new CoListJazzApi(proxy, raw, sourceSchema),
        enumerable: false,
      },
      $isLoaded: { value: true, enumerable: false },
    });

    return proxy;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, seenAbove?: ID<CoValue>[]): any[] {
    const itemDescriptor = schemaFieldToFieldDescriptor(
      this.$jazz.sourceSchema.element as SchemaField,
    );
    if (itemDescriptor.type === "json") {
      return this.$jazz.raw.asArray();
    } else if (itemDescriptor.type === "encoded") {
      return this.$jazz.raw.asArray().map((e) => itemDescriptor.encode(e));
    } else if (isRefEncoded(itemDescriptor)) {
      return this.map((item, idx) =>
        item && seenAbove?.includes(item.$jazz.id)
          ? { _circular: item?.$jazz.id }
          : (item as unknown as CoValue)?.toJSON(idx + "", [
              ...(seenAbove || []),
              this.$jazz.id,
            ]),
      );
    } else {
      return [];
    }
  }

  [inspect]() {
    return this.toJSON();
  }

  // Override mutation methods defined on Array, as CoLists aren't meant to be mutated directly
  /**
   * @deprecated Use `.$jazz.push` instead.
   */
  override push(...items: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.push` instead.",
    );
  }
  /**
   * @deprecated Use `.$jazz.unshift` instead.
   */
  override unshift(...items: never): number {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.unshift` instead.",
    );
  }
  /**
   * @deprecated Use `.$jazz.pop` instead.
   */
  // @ts-expect-error
  override pop(value: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.pop` instead.",
    );
  }
  /**
   * @deprecated Use `.$jazz.shift` instead.
   */
  // @ts-expect-error
  override shift(value: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.shift` instead.",
    );
  }
  /**
   * @deprecated Use `.$jazz.splice` instead.
   */
  override splice(start: never, deleteCount: never, ...items: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.splice` instead.",
    );
  }
  /**
   * @deprecated Use `.$jazz.set` instead.
   */
  override copyWithin(target: never, start: never, end: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.set` instead.",
    );
  }
  /**
   * @deprecated Use `.$jazz.set` instead.
   */
  override fill(value: never, start?: never, end?: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.$jazz.set` instead.",
    );
  }
  /**
   * @deprecated Use `.toReversed` if you want a reversed copy, or `.$jazz.set` to mutate the CoList.
   */
  // @ts-expect-error
  override reverse(value: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.toReversed` if you want a reversed copy, or `.$jazz.set` to mutate the CoList.",
    );
  }
  /**
   * @deprecated Use `.toSorted()` if you want a sorted copy, or `.$jazz.set` to mutate the CoList.
   */
  override sort(compareFn?: never): never {
    throw new Error(
      "Cannot mutate a CoList directly. Use `.toSorted` if you want a sorted copy, or `.$jazz.set` to mutate the CoList.",
    );
  }
}

export class CoListJazzApi<
  L extends CoList<S>,
  S extends CoreCoListSchema,
> extends CoValueJazzApi<L> {
  constructor(
    private coList: L,
    public raw: RawCoList,
    /** @internal */
    public sourceSchema: S,
  ) {
    super(coList);
  }

  /** @category Collaboration */
  get owner(): Group {
    return getCoValueOwner(this.coList);
  }

  set(index: number, value: CoFieldInit<S["element"]>): void {
    const itemDescriptor = schemaFieldToFieldDescriptor(
      this.sourceSchema.element as SchemaField,
    );
    const rawValue = toRawItems([value], itemDescriptor, this.owner)[0]!;
    if (rawValue === null && !itemDescriptor.optional) {
      throw new Error(`Cannot set required reference ${index} to undefined`);
    }
    this.raw.replace(index, rawValue);
  }

  /**
   * Appends new elements to the end of an array, and returns the new length of the array.
   * @param items New elements to add to the array.
   *
   * @category Content
   */
  push(...items: CoFieldInit<S["element"]>[]): number {
    const itemDescriptor = schemaFieldToFieldDescriptor(
      this.sourceSchema.element as SchemaField,
    );
    this.raw.appendItems(
      toRawItems(items, itemDescriptor, this.owner),
      undefined,
      "private",
    );

    return this.raw.entries().length;
  }

  /**
   * Inserts new elements at the start of an array, and returns the new length of the array.
   * @param items Elements to insert at the start of the array.
   *
   * @category Content
   */
  unshift(...items: CoFieldInit<S["element"]>[]): number {
    const itemDescriptor = schemaFieldToFieldDescriptor(
      this.sourceSchema.element as SchemaField,
    );
    for (const item of toRawItems(
      items as CoFieldInit<S["element"]>[],
      itemDescriptor,
      this.owner,
    )) {
      this.raw.prepend(item);
    }

    return this.raw.entries().length;
  }

  /**
   * Removes the last element from an array and returns it.
   * If the array is empty, undefined is returned and the array is not modified.
   *
   * @category Content
   */
  pop(): PrimitiveOrInaccessible<S["element"]> | undefined {
    const last = this.coList[this.coList.length - 1];

    this.raw.delete(this.coList.length - 1);

    return last;
  }

  /**
   * Removes the first element from an array and returns it.
   * If the array is empty, undefined is returned and the array is not modified.
   *
   * @category Content
   */
  shift(): PrimitiveOrInaccessible<S["element"]> | undefined {
    const first = this.coList[0];

    this.raw.delete(0);

    return first;
  }

  /**
   * Removes elements from an array and, if necessary, inserts new elements in their place, returning the deleted elements.
   * @param start The zero-based location in the array from which to start removing elements.
   * @param deleteCount The number of elements to remove.
   * @param items Elements to insert into the array in place of the deleted elements.
   * @returns An array containing the elements that were deleted.
   *
   * @category Content
   */
  splice(
    start: number,
    deleteCount: number,
    ...items: CoFieldInit<S["element"]>[]
  ): PrimitiveOrInaccessible<S["element"]>[] {
    const deleted = this.coList.slice(start, start + deleteCount);

    for (
      let idxToDelete = start + deleteCount - 1;
      idxToDelete >= start;
      idxToDelete--
    ) {
      this.raw.delete(idxToDelete);
    }

    const itemDescriptor = schemaFieldToFieldDescriptor(
      this.sourceSchema.element as SchemaField,
    );
    const rawItems = toRawItems(
      items as CoFieldInit<S["element"]>[],
      itemDescriptor,
      this.owner,
    );

    // If there are no items to insert, return the deleted items
    if (rawItems.length === 0) {
      return deleted;
    }

    // Fast path for single item insertion
    if (rawItems.length === 1) {
      const item = rawItems[0];
      if (item === undefined) return deleted;
      if (start === 0) {
        this.raw.prepend(item);
      } else {
        this.raw.append(item, Math.max(start - 1, 0));
      }
      return deleted;
    }

    // Handle multiple items
    if (start === 0) {
      // Iterate in reverse order without creating a new array
      for (let i = rawItems.length - 1; i >= 0; i--) {
        const item = rawItems[i];
        if (item === undefined) continue;
        this.raw.prepend(item);
      }
    } else {
      let appendAfter = Math.max(start - 1, 0);
      for (const item of rawItems) {
        if (item === undefined) continue;
        this.raw.append(item, appendAfter);
        appendAfter++;
      }
    }

    return deleted;
  }

  /**
   * Removes the elements at the specified indices from the array.
   * @param indices The indices of the elements to remove.
   * @returns The removed elements.
   *
   * @category Content
   */
  remove(...indices: number[]): PrimitiveOrInaccessible<S["element"]>[];
  /**
   * Removes the elements matching the predicate from the array.
   * @param predicate The predicate to match the elements to remove.
   * @returns The removed elements.
   *
   * @category Content
   */
  remove(
    predicate: (
      item: PrimitiveOrInaccessible<S["element"]>,
      index: number,
      coList: L,
    ) => boolean,
  ): PrimitiveOrInaccessible<S["element"]>[];
  remove(
    ...args: (
      | number
      | ((
          item: PrimitiveOrInaccessible<S["element"]>,
          index: number,
          coList: L,
        ) => boolean)
    )[]
  ): PrimitiveOrInaccessible<S["element"]>[] {
    const predicate = args[0] instanceof Function ? args[0] : undefined;
    let indices: number[] = [];
    if (predicate) {
      for (let i = 0; i < this.coList.length; i++) {
        if (predicate(this.coList[i]!, i, this.coList)) {
          indices.push(i);
        }
      }
    } else {
      indices = (args as number[])
        .filter((index) => index >= 0 && index < this.coList.length)
        .sort((a, b) => a - b);
    }
    const deletedItems = indices.map((index) => this.coList[index]!);
    for (const index of indices.reverse()) {
      this.raw.delete(index);
    }
    return deletedItems;
  }

  /**
   * Retains only the elements matching the predicate from the array.
   * @param predicate The predicate to match the elements to retain.
   * @returns The removed elements.
   *
   * @category Content
   */
  retain(
    predicate: (
      item: PrimitiveOrInaccessible<S["element"]>,
      index: number,
      coList: L,
    ) => boolean,
  ): PrimitiveOrInaccessible<S["element"]>[] {
    return this.remove((...args) => !predicate(...args));
  }

  /**
   * Modify the `CoList` to match another list, where the changes are managed internally.
   *
   * Changes are detected using `Object.is` for non-collaborative values and `$jazz.id` for collaborative values.
   *
   * @param result - The resolved list of items. For collaborative values, both CoValues and JSON values are supported.
   * @returns The modified CoList.
   *
   * @category Content
   */
  applyDiff(result: CoFieldInit<S["element"]>[]): L {
    const current = this.raw.asArray() as CoFieldInit<S["element"]>[];
    const itemDescriptor = schemaFieldToFieldDescriptor(
      this.sourceSchema.element as SchemaField,
    );
    const comparator = isRefEncoded(itemDescriptor)
      ? (aIdx: number, bIdx: number) => {
          const oldCoValueId = (current[aIdx] as CoValue)?.$jazz?.id;
          const newCoValueId = (result[bIdx] as CoValue)?.$jazz?.id;
          const isSame =
            !!oldCoValueId && !!newCoValueId && oldCoValueId === newCoValueId;
          return isSame;
        }
      : undefined;

    const patches = [...calcPatch(current, result, comparator)];

    if (patches.length === 0) {
      return this.coList;
    }

    // Turns off updates in the middle of applyDiff to improve the performance
    this.raw.core.pauseNotifyUpdate();

    for (const [from, to, insert] of patches.reverse()) {
      this.splice(from, to - from, ...insert);
    }

    this.raw.core.resumeNotifyUpdate();

    return this.coList;
  }

  /**
   * Given an already loaded `CoList`, ensure that items are loaded to the specified depth.
   *
   * Works like `CoList.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<L extends CoList<S>, const R extends ResolveQuery<S>>(
    this: CoListJazzApi<L, S>,
    options: {
      resolve: ResolveQueryStrict<S, R>;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<S, R>> {
    return ensureCoValueLoaded(this.coList, options);
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
  subscribe<L extends CoList<S>, const R extends ResolveQuery<S> = true>(
    this: CoListJazzApi<L, S>,
    listener: (value: Loaded<S, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<L extends CoList<S>, const R extends ResolveQuery<S> = true>(
    this: CoListJazzApi<L, S>,
    options: {
      resolve?: ResolveQueryStrict<S, R>;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: Loaded<S, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<L extends CoList<S>, const R extends ResolveQuery<S>>(
    this: CoListJazzApi<L, S>,
    ...args: SubscribeRestArgs<S, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this.coList, options, listener);
  }

  /**
   * Wait for the `CoList` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  async waitForSync(options?: { timeout?: number }): Promise<void> {
    await this.raw.core.waitForSync(options);
  }

  /**
   * If a `CoList`'s items are a `coField.ref(...)`, you can use `coList.$jazz.refs[i]` to access
   * the `Ref` instead of the potentially loaded/null value.
   *
   * This allows you to always get the ID or load the value manually.
   *
   * @example
   * ```ts
   * animals.$jazz.refs[0].id; // => ID<Animal>
   * animals.$jazz.refs[0].value;
   * // => Animal | null
   * const animal = await animals.$jazz.refs[0].load();
   * ```
   *
   * @category Content
   **/
  get refs(): {
    [idx: number]: S["element"] extends CoreCoValueSchema ? Ref<S> : never;
  } & {
    length: number;
    [Symbol.iterator](): IterableIterator<
      S["element"] extends CoreCoValueSchema ? Ref<S["element"]> : never
    >;
  } {
    return makeRefs<number>(
      this.coList,
      (idx) => this.raw.get(idx) as unknown as ID<CoValue>,
      () => Array.from({ length: this.raw.entries().length }, (_, idx) => idx),
      this.loadedAs,
      (_idx) => {
        return schemaFieldToFieldDescriptor(
          this.sourceSchema.element as SchemaField,
        ) as RefEncoded<CoreCoValueSchema>;
      },
    ) as any;
  }

  /**
   * Get the edits made to the CoList.
   *
   * @category Collaboration
   */
  getEdits(): {
    [idx: number]: {
      value?: PrimitiveOrInaccessible<S["element"]>;
      ref?: S["element"] extends CoreCoValueSchema ? Ref<S["element"]> : never;
      by: MaybeLoaded<CoreAccountSchema> | null;
      madeAt: Date;
    };
  } {
    throw new Error("Not implemented");
  }
}

/**
 * Convert an array of items to a raw array of items.
 * @param items - The array of items to convert.
 * @param itemDescriptor - The descriptor of the items.
 * @param owner - The owner of the CoList.
 * @returns The raw array of items.
 */
export function toRawItems<Item>(
  items: Item[],
  itemDescriptor: FieldDescriptor,
  owner: Group,
): JsonValue[] {
  let rawItems: JsonValue[] = [];
  if (itemDescriptor.type === "json") {
    rawItems = items as JsonValue[];
  } else if (itemDescriptor.type === "encoded") {
    rawItems = items?.map((e) => itemDescriptor.encode(e));
  } else if (isRefEncoded(itemDescriptor)) {
    rawItems = items?.map((value) => {
      if (value == null) {
        return null;
      }
      let refId = (value as unknown as CoValue).$jazz?.id;
      if (!refId) {
        const newOwnerStrategy =
          itemDescriptor.permissions?.newInlineOwnerStrategy;
        const onCreate = itemDescriptor.permissions?.onCreate;
        const coValue = instantiateRefEncodedWithInit(
          itemDescriptor,
          value,
          owner,
          newOwnerStrategy,
          onCreate,
        );
        refId = coValue.$jazz.id;
      }
      return refId;
    });
  } else {
    throw new Error("Invalid element descriptor");
  }
  return rawItems;
}

const CoListProxyHandler: ProxyHandler<CoList<CoreCoListSchema>> = {
  get(target, key, receiver) {
    if (typeof key === "string" && !isNaN(+key)) {
      const itemDescriptor = schemaFieldToFieldDescriptor(
        target.$jazz.sourceSchema.element as SchemaField,
      );
      const rawValue = target.$jazz.raw.get(Number(key));
      if (itemDescriptor.type === "json") {
        return rawValue;
      } else if (itemDescriptor.type === "encoded") {
        return rawValue === undefined
          ? undefined
          : itemDescriptor.decode(rawValue);
      } else if (isRefEncoded(itemDescriptor)) {
        return rawValue === undefined || rawValue === null
          ? undefined
          : accessChildByKey(target, rawValue as string, key);
      }
    } else if (key === "length") {
      return target.$jazz.raw.entries().length;
    } else {
      return Reflect.get(target, key, receiver);
    }
  },
  set(target, key, value, receiver) {
    if (typeof key === "string" && !isNaN(+key)) {
      throw Error("Cannot update a CoList directly. Use `$jazz.set` instead.");
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  has(target, key) {
    if (typeof key === "string" && !isNaN(+key)) {
      return Number(key) < target.$jazz.raw.entries().length;
    } else {
      return Reflect.has(target, key);
    }
  },
  ownKeys(target) {
    const keys = Reflect.ownKeys(target);
    // Add numeric indices for all entries in the list
    const indexKeys = target.$jazz.raw.entries().map((_entry, i) => String(i));
    keys.push(...indexKeys);
    return keys;
  },
  getOwnPropertyDescriptor(target, key) {
    if (key === TypeSym) {
      // Make TypeSym non-enumerable so it doesn't show up in Object.keys()
      return {
        enumerable: false,
        configurable: true,
        writable: false,
        value: target[TypeSym],
      };
    } else if (key in target) {
      return Reflect.getOwnPropertyDescriptor(target, key);
    } else if (typeof key === "string" && !isNaN(+key)) {
      const index = Number(key);
      if (index >= 0 && index < target.$jazz.raw.entries().length) {
        return {
          enumerable: true,
          configurable: true,
          writable: true,
        };
      }
    } else if (key === "length") {
      return {
        enumerable: false,
        configurable: false,
        writable: false,
      };
    }
  },
};

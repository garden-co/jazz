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
  RefEncoded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  SchemaFor,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
} from "../internal.js";
import {
  AnonymousJazzAgent,
  ItemsSym,
  Ref,
  SchemaInit,
  accessChildByKey,
  activeAccountContext,
  coField,
  ensureCoValueLoaded,
  inspect,
  instantiateRefEncodedWithInit,
  isRefEncoded,
  loadCoValueWithoutMe,
  makeRefs,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
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
export class CoList<out Item = any>
  extends Array<Item>
  implements ReadonlyArray<Item>, CoValue
{
  declare $jazz: CoListJazzApi<this>;

  /**
   * Declare a `CoList` by subclassing `CoList.Of(...)` and passing the item schema using `co`.
   *
   * @example
   * ```ts
   * class ColorList extends CoList.Of(
   *   coField.string
   * ) {}
   * class AnimalList extends CoList.Of(
   *   coField.ref(Animal)
   * ) {}
   * ```
   *
   * @category Declaration
   */
  static Of<Item>(item: Item): typeof CoList<Item> {
    // TODO: cache superclass for item class
    return class CoListOf extends CoList<Item> {
      [coField.items] = item;
    };
  }

  /**
   * @ignore
   * @deprecated Use UPPERCASE `CoList.Of` instead! */
  static of(..._args: never): never {
    throw new Error("Can't use Array.of with CoLists");
  }

  /** @category Type Helpers */
  declare [TypeSym]: "CoList";
  static {
    this.prototype[TypeSym] = "CoList";
  }

  /** @internal This is only a marker type and doesn't exist at runtime */
  [ItemsSym]!: Item;
  /** @internal */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;

  static get [Symbol.species]() {
    return Array;
  }

  constructor(options: { fromRaw: RawCoList } | undefined) {
    super();

    const proxy = new Proxy(this, CoListProxyHandler as ProxyHandler<this>);

    if (options && "fromRaw" in options) {
      Object.defineProperties(this, {
        $jazz: {
          value: new CoListJazzApi(proxy, () => options.fromRaw),
          enumerable: false,
        },
      });
    }

    return proxy;
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
   * @deprecated Use `co.list(...).create` instead.
   **/
  static create<L extends CoList>(
    this: CoValueClass<L>,
    items: L[number][],
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    const instance = new this();

    Object.defineProperties(instance, {
      $jazz: {
        value: new CoListJazzApi(instance, () => raw),
        enumerable: false,
      },
    });

    const raw = owner.$jazz.raw.createList(
      toRawItems(items, instance.$jazz.schema[ItemsSym], owner),
      null,
      "private",
      uniqueness,
    );

    return instance;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, seenAbove?: ID<CoValue>[]): any[] {
    const itemDescriptor = this.$jazz.schema[ItemsSym] as Schema;
    if (itemDescriptor === "json") {
      return this.$jazz.raw.asArray();
    } else if ("encoded" in itemDescriptor) {
      return this.$jazz.raw
        .asArray()
        .map((e) => itemDescriptor.encoded.encode(e));
    } else if (isRefEncoded(itemDescriptor)) {
      return this.map((item, idx) =>
        seenAbove?.includes((item as CoValue)?.$jazz.id)
          ? { _circular: (item as CoValue).$jazz.id }
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
    def: { [ItemsSym]: V["$jazz"]["schema"][ItemsSym] },
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
   * @deprecated Use `co.list(...).load` instead.
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
   * @deprecated Use `co.list(...).subscribe` instead.
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

  /** @deprecated Use `CoList.upsertUnique` and `CoList.loadUnique` instead. */
  static findUnique<L extends CoList>(
    this: CoValueClass<L>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ) {
    return CoList._findUnique(unique, ownerID, as);
  }

  /** @internal */
  static _findUnique<L extends CoList>(
    this: CoValueClass<L>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ) {
    as ||= activeAccountContext.get();

    const header = {
      type: "colist" as const,
      ruleset: {
        type: "ownedByGroup" as const,
        group: ownerID as RawCoID,
      },
      meta: null,
      uniqueness: unique,
    };
    const crypto =
      as[TypeSym] === "Anonymous" ? as.node.crypto : as.$jazz.localNode.crypto;
    return cojsonInternals.idforHeader(header, crypto) as ID<L>;
  }

  /**
   * Given some data, updates an existing CoList or initialises a new one if none exists.
   *
   * Note: This method respects resolve options, and thus can return `null` if the references cannot be resolved.
   *
   * @example
   * ```ts
   * const activeItems = await ItemList.upsertUnique(
   *   {
   *     value: [item1, item2, item3],
   *     unique: sourceData.identifier,
   *     owner: workspace,
   *   }
   * );
   * ```
   *
   * @param options The options for creating or loading the CoList. This includes the intended state of the CoList, its unique identifier, its owner, and the references to resolve.
   * @returns Either an existing & modified CoList, or a new initialised CoList if none exists.
   * @category Subscription & Loading
   */
  static async upsertUnique<
    L extends CoList,
    const R extends RefsToResolve<L> = true,
  >(
    this: CoValueClass<L>,
    options: {
      value: L[number][];
      unique: CoValueUniqueness["uniqueness"];
      owner: Account | Group;
      resolve?: RefsToResolveStrict<L, R>;
    },
  ): Promise<Resolved<L, R> | null> {
    const listId = CoList._findUnique(
      options.unique,
      options.owner.$jazz.id,
      options.owner.$jazz.loadedAs,
    );
    let list: Resolved<L, R> | null = await loadCoValueWithoutMe(this, listId, {
      ...options,
      loadAs: options.owner.$jazz.loadedAs,
      skipRetry: true,
    });
    if (!list) {
      list = (this as any).create(options.value, {
        owner: options.owner,
        unique: options.unique,
      }) as Resolved<L, R>;
    } else {
      (list as L).$jazz.applyDiff(options.value);
    }

    return await loadCoValueWithoutMe(this, listId, {
      ...options,
      loadAs: options.owner.$jazz.loadedAs,
      skipRetry: true,
    });
  }

  /**
   * Loads a CoList by its unique identifier and owner's ID.
   * @param unique The unique identifier of the CoList to load.
   * @param ownerID The ID of the owner of the CoList.
   * @param options Additional options for loading the CoList.
   * @returns The loaded CoList, or null if unavailable.
   */
  static loadUnique<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: CoValueClass<L>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<L, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<L, R> | null> {
    return loadCoValueWithoutMe(
      this,
      CoList._findUnique(unique, ownerID, options?.loadAs),
      { ...options, skipRetry: true },
    );
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

/** @internal */
type CoListItem<L> = L extends CoList<unknown> ? L[number] : never;

export class CoListJazzApi<L extends CoList> extends CoValueJazzApi<L> {
  constructor(
    private coList: L,
    private getRaw: () => RawCoList,
  ) {
    super(coList);
  }

  /**
   * The ID of this `CoList`
   * @category Content
   */
  get id(): ID<L> {
    return this.raw.id;
  }

  /** @category Collaboration */
  get owner(): Group {
    return getCoValueOwner(this.coList);
  }

  set(index: number, value: CoFieldInit<CoListItem<L>>): void {
    const itemDescriptor = this.schema[ItemsSym];
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
  push(...items: CoFieldInit<CoListItem<L>>[]): number {
    this.raw.appendItems(
      toRawItems(items, this.schema[ItemsSym], this.owner),
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
  unshift(...items: CoFieldInit<CoListItem<L>>[]): number {
    for (const item of toRawItems(
      items as CoFieldInit<CoListItem<L>>[],
      this.schema[ItemsSym],
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
  pop(): CoListItem<L> | undefined {
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
  shift(): CoListItem<L> | undefined {
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
    ...items: CoFieldInit<CoListItem<L>>[]
  ): CoListItem<L>[] {
    const deleted = this.coList.slice(start, start + deleteCount);

    for (
      let idxToDelete = start + deleteCount - 1;
      idxToDelete >= start;
      idxToDelete--
    ) {
      this.raw.delete(idxToDelete);
    }

    const rawItems = toRawItems(
      items as CoListItem<L>[],
      this.schema[ItemsSym],
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
  remove(...indices: number[]): CoListItem<L>[];
  /**
   * Removes the elements matching the predicate from the array.
   * @param predicate The predicate to match the elements to remove.
   * @returns The removed elements.
   *
   * @category Content
   */
  remove(
    predicate: (item: CoListItem<L>, index: number, coList: L) => boolean,
  ): CoListItem<L>[];
  remove(
    ...args: (
      | number
      | ((item: CoListItem<L>, index: number, coList: L) => boolean)
    )[]
  ): CoListItem<L>[] {
    const predicate = args[0] instanceof Function ? args[0] : undefined;
    let indices: number[] = [];
    if (predicate) {
      for (let i = 0; i < this.coList.length; i++) {
        if (predicate(this.coList[i], i, this.coList)) {
          indices.push(i);
        }
      }
    } else {
      indices = (args as number[])
        .filter((index) => index >= 0 && index < this.coList.length)
        .sort((a, b) => a - b);
    }
    const deletedItems = indices.map((index) => this.coList[index]);
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
    predicate: (item: CoListItem<L>, index: number, coList: L) => boolean,
  ): CoListItem<L>[] {
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
  applyDiff(result: CoFieldInit<CoListItem<L>>[]): L {
    const current = this.raw.asArray() as CoFieldInit<CoListItem<L>>[];
    const comparator = isRefEncoded(this.schema[ItemsSym])
      ? (aIdx: number, bIdx: number) => {
          const oldCoValueId = (current[aIdx] as CoValue)?.$jazz?.id;
          const newCoValueId = (result[bIdx] as CoValue)?.$jazz?.id;
          const isSame =
            !!oldCoValueId && !!newCoValueId && oldCoValueId === newCoValueId;
          return isSame;
        }
      : undefined;

    const patches = [...calcPatch(current, result, comparator)];
    for (const [from, to, insert] of patches.reverse()) {
      this.splice(from, to - from, ...insert);
    }
    return this.coList;
  }

  /**
   * Given an already loaded `CoList`, ensure that items are loaded to the specified depth.
   *
   * Works like `CoList.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<L extends CoList, const R extends RefsToResolve<L>>(
    this: CoListJazzApi<L>,
    options: { resolve: RefsToResolveStrict<L, R> },
  ): Promise<Resolved<L, R>> {
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
  subscribe<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: CoListJazzApi<L>,
    listener: (value: Resolved<L, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<L extends CoList, const R extends RefsToResolve<L> = true>(
    this: CoListJazzApi<L>,
    options: { resolve?: RefsToResolveStrict<L, R> },
    listener: (value: Resolved<L, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<L extends CoList, const R extends RefsToResolve<L>>(
    this: CoListJazzApi<L>,
    ...args: SubscribeRestArgs<L, R>
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
   * Get the descriptor for the items in the `CoList`
   * @internal
   */
  getItemsDescriptor(): Schema | undefined {
    return this.schema[ItemsSym];
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
    [idx: number]: Exclude<CoListItem<L>, null> extends CoValue
      ? Ref<Exclude<CoListItem<L>, null>>
      : never;
  } & {
    length: number;
    [Symbol.iterator](): IterableIterator<
      Exclude<CoListItem<L>, null> extends CoValue
        ? Ref<Exclude<CoListItem<L>, null>>
        : never
    >;
  } {
    return makeRefs<number>(
      this.coList,
      (idx) => this.raw.get(idx) as unknown as ID<CoValue>,
      () => Array.from({ length: this.raw.entries().length }, (_, idx) => idx),
      this.loadedAs,
      (_idx) => this.schema[ItemsSym] as RefEncoded<CoValue>,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ) as any;
  }

  /**
   * Get the edits made to the CoList.
   *
   * @category Collaboration
   */
  getEdits(): {
    [idx: number]: {
      value?: CoListItem<L>;
      ref?: CoListItem<L> extends CoValue ? Ref<CoListItem<L>> : never;
      by: Account | null;
      madeAt: Date;
    };
  } {
    throw new Error("Not implemented");
  }

  /** @internal */
  get raw(): RawCoList {
    return this.getRaw();
  }

  /** @internal */
  get schema(): {
    [ItemsSym]: SchemaFor<CoListItem<L>> | any;
  } {
    return (this.coList.constructor as typeof CoList)._schema;
  }
}

/**
 * Convert an array of items to a raw array of items.
 * @param items - The array of items to convert.
 * @param itemDescriptor - The descriptor of the items.
 * @param owner - The owner of the CoList.
 * @returns The raw array of items.
 */
function toRawItems<Item>(
  items: Item[],
  itemDescriptor: Schema,
  owner: Group,
): JsonValue[] {
  let rawItems: JsonValue[] = [];
  if (itemDescriptor === "json") {
    rawItems = items as JsonValue[];
  } else if ("encoded" in itemDescriptor) {
    rawItems = items?.map((e) => itemDescriptor.encoded.encode(e));
  } else if (isRefEncoded(itemDescriptor)) {
    rawItems = items?.map((value) => {
      if (value == null) {
        return null;
      }
      let refId = (value as unknown as CoValue).$jazz?.id;
      if (!refId) {
        const coValue = instantiateRefEncodedWithInit(
          itemDescriptor,
          value,
          owner,
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

const CoListProxyHandler: ProxyHandler<CoList> = {
  get(target, key, receiver) {
    if (typeof key === "string" && !isNaN(+key)) {
      const itemDescriptor = target.$jazz.schema[ItemsSym] as Schema;
      const rawValue = target.$jazz.raw.get(Number(key));
      if (itemDescriptor === "json") {
        return rawValue;
      } else if ("encoded" in itemDescriptor) {
        return rawValue === undefined
          ? undefined
          : itemDescriptor.encoded.decode(rawValue);
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
    if (key === ItemsSym && typeof value === "object" && SchemaInit in value) {
      (target.constructor as typeof CoList)._schema ||= {};
      (target.constructor as typeof CoList)._schema[ItemsSym] =
        value[SchemaInit];
      return true;
    }
    if (typeof key === "string" && !isNaN(+key)) {
      throw Error("Cannot update a CoList directly. Use `$jazz.set` instead.");
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
      return Number(key) < target.$jazz.raw.entries().length;
    } else {
      return Reflect.has(target, key);
    }
  },
};

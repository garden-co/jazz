import type { CoValueUniqueness, JsonValue, RawCoID, RawCoList } from "cojson";
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
  RefEncoded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  BranchDefinition,
  getIdFromHeader,
  getUniqueHeader,
  internalLoadUnique,
  AnonymousJazzAgent,
  ItemsSym,
  Ref,
  accessChildByKey,
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
  CoValueCreateOptionsInternal,
} from "../internal.js";
import { z } from "../implementation/zodSchema/zodReExport.js";
import { CoreCoListSchema } from "../implementation/zodSchema/schemaTypes/CoListSchema.js";
import {
  executeValidation,
  resolveValidationMode,
  type LocalValidationMode,
} from "../implementation/zodSchema/validationSettings.js";
import {
  expectArraySchema,
  normalizeZodSchema,
} from "../implementation/zodSchema/schemaTypes/schemaValidators.js";
import { assertCoValueSchema } from "../implementation/zodSchema/schemaInvariant.js";

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
  static coValueSchema?: CoreCoListSchema;
  declare $jazz: CoListJazzApi<this>;
  declare $isLoaded: true;

  /** @category Type Helpers */
  declare [TypeSym]: "CoList";
  static {
    this.prototype[TypeSym] = "CoList";
  }

  /** @internal This is only a marker type and doesn't exist at runtime */
  [ItemsSym]!: Item;
  static get [Symbol.species]() {
    return Array;
  }

  constructor(options: { fromRaw: RawCoList } | undefined) {
    super();

    const proxy = new Proxy(this, CoListProxyHandler as ProxyHandler<this>);

    if (options && "fromRaw" in options) {
      const coListSchema = assertCoValueSchema(
        this.constructor,
        "CoList",
        "load",
      );
      Object.defineProperties(this, {
        $jazz: {
          value: new CoListJazzApi(proxy, () => options.fromRaw, coListSchema),
          enumerable: false,
          configurable: true,
        },
        $isLoaded: { value: true, enumerable: false, configurable: true },
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
    options?: CoValueCreateOptionsInternal,
  ) {
    const coListSchema = assertCoValueSchema(this, "CoList", "create");
    const validationMode = resolveValidationMode(
      options && "validation" in options ? options.validation : undefined,
    );

    if (validationMode !== "loose") {
      executeValidation(
        coListSchema.getValidationSchema(),
        items,
        validationMode,
      ) as typeof items;
    }

    const instance = new this();
    const { owner, uniqueness, firstComesWins, restrictDeletion } =
      parseCoValueCreateOptions(options);

    Object.defineProperties(instance, {
      $jazz: {
        value: new CoListJazzApi(instance, () => raw, coListSchema),
        enumerable: false,
        configurable: true,
      },
      $isLoaded: { value: true, enumerable: false, configurable: true },
    });

    const initMeta = firstComesWins ? { fww: "init" } : undefined;
    const itemDescriptor = instance.$jazz.getItemsDescriptor();
    const raw = owner.$jazz.raw.createList(
      toRawItems(
        items,
        itemDescriptor,
        owner,
        firstComesWins,
        uniqueness?.uniqueness,
        options && "validation" in options ? options.validation : undefined,
      ),
      null,
      "private",
      uniqueness,
      initMeta,
      restrictDeletion ? { restrictDeletion: true } : undefined,
    );

    return instance;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, seenAbove?: ID<CoValue>[]): any[] {
    const itemDescriptor = this.$jazz.getItemsDescriptor();
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
  ): Promise<Settled<Resolved<L, R>>> {
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
    const header = getUniqueHeader("colist", unique, ownerID);
    return getIdFromHeader(header, as);
  }

  /**
   * Get an existing unique CoList or create a new one if it doesn't exist.
   *
   * Unlike `upsertUnique`, this method does NOT update existing values with the provided value.
   * The provided value is only used when creating a new CoList.
   *
   * @example
   * ```ts
   * const items = await ItemList.getOrCreateUnique({
   *   value: [item1, item2, item3],
   *   unique: ["user-items", me.id],
   *   owner: me,
   * });
   * ```
   *
   * @param options The options for creating or loading the CoList.
   * @returns Either an existing CoList (unchanged), or a new initialised CoList if none exists.
   * @category Subscription & Loading
   */
  static async getOrCreateUnique<
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
  ): Promise<Settled<Resolved<L, R>>> {
    return internalLoadUnique(this, {
      type: "colist",
      unique: options.unique,
      owner: options.owner,
      resolve: options.resolve,
      onCreateWhenMissing: () => {
        (this as any).create(options.value, {
          owner: options.owner,
          unique: options.unique,
          firstComesWins: true,
        });
      },
      // No onUpdateWhenFound - key difference from upsertUnique
    });
  }

  /**
   * Given some data, updates an existing CoList or initialises a new one if none exists.
   *
   * Note: This method respects resolve options, and thus can return a not-loaded value if the references cannot be resolved.
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
   *
   * @deprecated Use `getOrCreateUnique` instead. Note: getOrCreateUnique does not update existing values.
   * If you need to update, use getOrCreateUnique followed by `$jazz.applyDiff`.
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
  ): Promise<Settled<Resolved<L, R>>> {
    return internalLoadUnique(this, {
      type: "colist",
      unique: options.unique,
      owner: options.owner,
      resolve: options.resolve,
      onCreateWhenMissing: () => {
        (this as any).create(options.value, {
          owner: options.owner,
          unique: options.unique,
        });
      },
      onUpdateWhenFound(value) {
        (value as Resolved<L>).$jazz.applyDiff(options.value);
      },
    });
  }

  /**
   * Loads a CoList by its unique identifier and owner's ID.
   * @param unique The unique identifier of the CoList to load.
   * @param ownerID The ID of the owner of the CoList.
   * @param options Additional options for loading the CoList.
   * @returns The loaded CoList, or an not-loaded value if unavailable.
   *
   * @category Subscription & Loading
   */
  static async loadUnique<
    L extends CoList,
    const R extends RefsToResolve<L> = true,
  >(
    this: CoValueClass<L>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<L, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Settled<Resolved<L, R>>> {
    const owner = await Group.load(ownerID, {
      loadAs: options?.loadAs,
    });
    if (!owner.$isLoaded) return owner;

    return internalLoadUnique(this, {
      type: "colist",
      unique,
      owner,
      resolve: options?.resolve,
    });
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
    private coListSchema: CoreCoListSchema,
  ) {
    super(coList);
  }

  private getItemSchema(): z.ZodType {
    const fieldSchema = expectArraySchema(
      this.coListSchema.getValidationSchema(),
    ).element;

    return normalizeZodSchema(fieldSchema);
  }

  /** @category Collaboration */
  get owner(): Group {
    return getCoValueOwner(this.coList);
  }

  set(
    index: number,
    value: CoFieldInit<CoListItem<L>>,
    options?: { validation?: LocalValidationMode },
  ): void {
    const validationMode = resolveValidationMode(options?.validation);
    if (validationMode !== "loose" && this.coListSchema) {
      const fieldSchema = this.getItemSchema();
      executeValidation(fieldSchema, value, validationMode) as CoFieldInit<
        CoListItem<L>
      >;
    }

    const itemDescriptor = this.getItemsDescriptor();
    const rawValue = toRawItems(
      [value],
      itemDescriptor,
      this.owner,
      undefined,
      undefined,
      options?.validation,
    )[0]!;
    if (
      rawValue === null &&
      isRefEncoded(itemDescriptor) &&
      !itemDescriptor.optional
    ) {
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
    const validationMode = resolveValidationMode();
    if (validationMode !== "loose" && this.coListSchema) {
      const schema = z.array(this.getItemSchema());
      executeValidation(schema, items, validationMode) as CoFieldInit<
        CoListItem<L>
      >[];
    }
    return this.pushLoose(...items);
  }

  /**
   * Appends new elements to the end of an array, and returns the new length of the array.
   * Schema validation is not applied to the items.
   * @param items New elements to add to the array.
   *
   * @category Content
   */
  pushLoose(...items: CoFieldInit<CoListItem<L>>[]): number {
    this.raw.appendItems(
      toRawItems(
        items,
        this.getItemsDescriptor(),
        this.owner,
        undefined,
        undefined,
        "loose",
      ),
      undefined,
      "private",
    );

    return this.raw.length();
  }

  /**
   * Inserts new elements at the start of an array, and returns the new length of the array.
   * @param items Elements to insert at the start of the array.
   *
   * @category Content
   */
  unshift(...items: CoFieldInit<CoListItem<L>>[]): number {
    const validationMode = resolveValidationMode();
    if (validationMode !== "loose" && this.coListSchema) {
      const schema = z.array(this.getItemSchema());
      executeValidation(schema, items, validationMode) as CoFieldInit<
        CoListItem<L>
      >[];
    }
    return this.unshiftLoose(...items);
  }

  /**
   * Inserts new elements at the start of an array, and returns the new length of the array.
   * Schema validation is not applied to the items.
   * @param items Elements to insert at the start of the array.
   *
   * @category Content
   */
  unshiftLoose(...items: CoFieldInit<CoListItem<L>>[]): number {
    for (const item of toRawItems(
      items as CoFieldInit<CoListItem<L>>[],
      this.getItemsDescriptor(),
      this.owner,
      undefined,
      undefined,
      "loose",
    )) {
      this.raw.prepend(item);
    }

    return this.raw.length();
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
   * Items are validated using the schema.
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
    const validationMode = resolveValidationMode();
    if (validationMode !== "loose" && this.coListSchema) {
      const schema = z.array(this.getItemSchema());
      executeValidation(schema, items, validationMode) as CoFieldInit<
        CoListItem<L>
      >[];
    }

    return this.spliceLoose(start, deleteCount, ...items);
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
  spliceLoose(
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
      this.getItemsDescriptor(),
      this.owner,
      undefined,
      undefined,
      "loose",
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
  applyDiff(
    result: CoFieldInit<CoListItem<L>>[],
    options?: { validation?: LocalValidationMode },
  ): L {
    const validationMode = resolveValidationMode(options?.validation);
    if (validationMode !== "loose" && this.coListSchema) {
      const schema = z.array(this.getItemSchema());
      executeValidation(schema, result, validationMode) as CoFieldInit<
        CoListItem<L>
      >[];
    }
    const current = this.raw.asArray() as CoFieldInit<CoListItem<L>>[];
    const comparator = isRefEncoded(this.getItemsDescriptor())
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
      this.spliceLoose(from, to - from, ...insert);
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
  ensureLoaded<L extends CoList, const R extends RefsToResolve<L>>(
    this: CoListJazzApi<L>,
    options: {
      resolve: RefsToResolveStrict<L, R>;
      unstable_branch?: BranchDefinition;
    },
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
    options: {
      resolve?: RefsToResolveStrict<L, R>;
      unstable_branch?: BranchDefinition;
    },
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
  getItemsDescriptor(): Schema {
    return this.coListSchema.getDescriptorsSchema();
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
    [idx: number]: AsLoaded<CoListItem<L>> extends CoValue
      ? Ref<AsLoaded<CoListItem<L>>>
      : never;
  } & {
    length: number;
    [Symbol.iterator](): IterableIterator<
      AsLoaded<CoListItem<L>> extends CoValue
        ? Ref<AsLoaded<CoListItem<L>>>
        : never
    >;
  } {
    return makeRefs<number>(
      this.coList,
      (idx) => this.raw.get(idx) as unknown as ID<CoValue>,
      () => Array.from({ length: this.raw.entries().length }, (_, idx) => idx),
      this.loadedAs,
      (_idx) => this.getItemsDescriptor() as RefEncoded<CoValue>,
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
  firstComesWins = false,
  uniqueness?: CoValueUniqueness["uniqueness"],
  validationMode?: LocalValidationMode,
): JsonValue[] {
  let rawItems: JsonValue[] = [];
  if (itemDescriptor === "json") {
    rawItems = items as JsonValue[];
  } else if ("encoded" in itemDescriptor) {
    rawItems = items?.map((e) => itemDescriptor.encoded.encode(e));
  } else if (isRefEncoded(itemDescriptor)) {
    rawItems = items?.map((value, index) => {
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
          uniqueness
            ? { uniqueness: uniqueness, fieldName: `${index}`, firstComesWins }
            : undefined,
          validationMode,
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

function getCoListItemValue(target: CoList, key: string) {
  const rawValue = target.$jazz.raw.get(Number(key));

  if (rawValue === undefined) {
    return undefined;
  }

  const itemDescriptor = target.$jazz.getItemsDescriptor();

  if (!itemDescriptor) {
    return undefined;
  }

  if (itemDescriptor === "json") {
    return rawValue;
  } else if ("encoded" in itemDescriptor) {
    return itemDescriptor.encoded.decode(rawValue);
  } else if (isRefEncoded(itemDescriptor)) {
    if (rawValue === null) {
      return undefined;
    }

    return accessChildByKey(target, rawValue as string, key);
  }

  return undefined;
}

const CoListProxyHandler: ProxyHandler<CoList> = {
  get(target, key, receiver) {
    if (typeof key === "symbol") {
      return Reflect.get(target, key, receiver);
    }

    if (!isNaN(+key)) {
      return getCoListItemValue(target, key);
    } else if (key === "length") {
      return target.$jazz.raw.length();
    }

    return Reflect.get(target, key, receiver);
  },
  set(target, key, value, receiver) {
    if (typeof key === "symbol") {
      return Reflect.set(target, key, value, receiver);
    }

    if (!isNaN(+key)) {
      throw Error("Cannot update a CoList directly. Use `$jazz.set` instead.");
    }

    return Reflect.set(target, key, value, receiver);
  },
  defineProperty(target, key, descriptor) {
    return Reflect.defineProperty(target, key, descriptor);
  },
  has(target, key) {
    if (typeof key === "string" && !isNaN(+key)) {
      return Number(key) < target.$jazz.raw.length();
    } else {
      return Reflect.has(target, key);
    }
  },
  ownKeys(target) {
    const keys = Reflect.ownKeys(target);
    // Add numeric indices for all entries in the list
    const length = target.$jazz.raw.length();
    for (let i = 0; i < length; i++) {
      keys.push(String(i));
    }
    return keys;
  },
  getOwnPropertyDescriptor(target, key) {
    if (typeof key === "symbol") {
      return Reflect.getOwnPropertyDescriptor(target, key);
    }

    if (key === TypeSym) {
      // Make TypeSym non-enumerable so it doesn't show up in Object.keys()
      return {
        enumerable: false,
        configurable: true,
        writable: false,
        value: target[TypeSym],
      };
    } else if (!isNaN(+key)) {
      const index = Number(key);
      if (index >= 0 && index < target.$jazz.raw.length()) {
        return {
          enumerable: true,
          configurable: true,
          writable: false,
          value: getCoListItemValue(target, key),
        };
      }
    } else if (key === "length") {
      return {
        enumerable: false,
        configurable: false,
        writable: true, // Must be writable, otherwise JS complains
        value: target.$jazz.raw.length(),
      };
    }
    return Reflect.getOwnPropertyDescriptor(target, key);
  },
};

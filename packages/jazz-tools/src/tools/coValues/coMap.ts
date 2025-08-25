import {
  AgentID,
  type CoValueUniqueness,
  CojsonInternalTypes,
  type JsonValue,
  RawAccountID,
  RawCoID,
  type RawCoMap,
  cojsonInternals,
} from "cojson";
import {
  AnonymousJazzAgent,
  CoFieldInit,
  CoValue,
  CoValueClass,
  getCoValueOwner,
  Group,
  ID,
  PartialOnUndefined,
  RefEncoded,
  RefIfCoValue,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  Simplify,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
} from "../internal.js";
import {
  Account,
  CoValueBase,
  CoValueJazzApi,
  ItemsSym,
  Ref,
  RegisteredSchemas,
  SchemaInit,
  accessChildById,
  accessChildByKey,
  activeAccountContext,
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

type CoMapEdit<V> = {
  value?: V;
  ref?: RefIfCoValue<V>;
  by: Account | null;
  madeAt: Date;
  key?: string;
};

type LastAndAllCoMapEdits<V> = CoMapEdit<V> & { all: CoMapEdit<V>[] };

type CoMapEdits<M extends CoMap> = {
  [Key in CoKeys<M>]?: LastAndAllCoMapEdits<M[Key]>;
};

type CoMapFieldSchema = {
  [key: string]: Schema;
} & { [ItemsSym]?: Schema };

/**
 * CoMaps are collaborative versions of plain objects, mapping string-like keys to values.
 *
 * @categoryDescription Declaration
 * Declare your own CoMap schemas by subclassing `CoMap` and assigning field schemas with `co`.
 *
 * Optional `coField.ref(...)` fields must be marked with `{ optional: true }`.
 *
 * ```ts
 * import { coField, CoMap } from "jazz-tools";
 *
 * class Person extends CoMap {
 *   name = coField.string;
 *   age = coField.number;
 *   pet = coField.ref(Animal);
 *   car = coField.ref(Car, { optional: true });
 * }
 * ```
 *
 * @categoryDescription Content
 * You can access properties you declare on a `CoMap` (using `co`) as if they were normal properties on a plain object, using dot notation, `Object.keys()`, etc.
 *
 * ```ts
 * person.name;
 * person["age"];
 * person.age = 42;
 * person.pet?.name;
 * Object.keys(person);
 * // => ["name", "age", "pet"]
 * ```
 *
 * @category CoValues
 *  */
export class CoMap extends CoValueBase implements CoValue {
  /** @category Type Helpers */
  declare [TypeSym]: "CoMap";
  static {
    this.prototype[TypeSym] = "CoMap";
  }

  /**
   * Jazz methods for CoMaps are inside this property.
   *
   * This allows CoMaps to be used as plain objects while still having
   * access to Jazz methods, and also doesn't limit which key names can be
   * used inside CoMaps.
   */
  declare $jazz: CoMapJazzApi<this>;

  /** @internal */
  static _schema: CoMapFieldSchema;

  /** @internal */
  constructor(options: { fromRaw: RawCoMap } | undefined) {
    super();

    const proxy = new Proxy(this, CoMapProxyHandler as ProxyHandler<this>);

    if (options) {
      if ("fromRaw" in options) {
        Object.defineProperties(this, {
          $jazz: {
            value: new CoMapJazzApi(proxy, () => options.fromRaw),
            enumerable: false,
          },
        });
      } else {
        throw new Error("Invalid CoMap constructor arguments");
      }
    }

    return proxy;
  }

  /**
   * Create a new CoMap with the given initial values and owner.
   *
   * The owner (a Group or Account) determines access rights to the CoMap.
   *
   * The CoMap will immediately be persisted and synced to connected peers.
   *
   * @example
   * ```ts
   * const person = Person.create({
   *   name: "Alice",
   *   age: 42,
   *   pet: cat,
   * }, { owner: friendGroup });
   * ```
   *
   * @category Creation
   *
   * @deprecated Use `co.map(...).create`.
   **/
  static create<M extends CoMap>(
    this: CoValueClass<M>,
    init: Simplify<CoMapInit_DEPRECATED<M>>,
    options?:
      | {
          owner?: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) {
    const instance = new this();

    return CoMap._createCoMap(instance, init, options);
  }

  /**
   * Return a JSON representation of the `CoMap`
   * @category Content
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, processedValues?: ID<CoValue>[]): any {
    const result = {} as Record<string, any>;

    for (const key of this.$jazz.raw.keys()) {
      const tKey = key as CoKeys<this>;
      const descriptor = this.$jazz.getDescriptor(tKey);

      if (!descriptor) {
        continue;
      }

      if (descriptor == "json" || "encoded" in descriptor) {
        result[key] = this.$jazz.raw.get(key);
      } else if (isRefEncoded(descriptor)) {
        const id = this.$jazz.raw.get(key) as ID<CoValue>;

        if (processedValues?.includes(id) || id === this.$jazz.id) {
          result[key] = { _circular: id };
          continue;
        }

        const ref = this[tKey];

        if (
          ref &&
          typeof ref === "object" &&
          "toJSON" in ref &&
          typeof ref.toJSON === "function"
        ) {
          const jsonedRef = ref.toJSON(tKey, [
            ...(processedValues || []),
            this.$jazz.id,
          ]);
          result[key] = jsonedRef;
        }
      } else {
        result[key] = undefined;
      }
    }

    return result;
  }

  [inspect]() {
    return this.toJSON();
  }

  /**
   * @internal
   */
  static _createCoMap<M extends CoMap>(
    instance: M,
    init: Simplify<CoMapInit_DEPRECATED<M>>,
    options?:
      | {
          owner?: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ): M {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);

    Object.defineProperties(instance, {
      $jazz: {
        value: new CoMapJazzApi(instance, () => raw),
        enumerable: false,
      },
    });

    const raw = CoMap.rawFromInit(instance, init, owner, uniqueness);

    return instance;
  }

  /**
   * Create a new `RawCoMap` from an initialization object
   * @internal
   */
  static rawFromInit<M extends CoMap, Fields extends object>(
    instance: M,
    init: Simplify<CoMapInit_DEPRECATED<Fields>> | undefined,
    owner: Group,
    uniqueness?: CoValueUniqueness,
  ) {
    const rawOwner = owner.$jazz.raw;

    const rawInit = {} as {
      [key in keyof Fields]: JsonValue | undefined;
    };

    if (init)
      for (const key of Object.keys(init) as (keyof Fields)[]) {
        const initValue = init[key as keyof typeof init];

        const descriptor = instance.$jazz.getDescriptor(key as string);

        if (!descriptor) {
          continue;
        }

        if (descriptor === "json") {
          rawInit[key] = initValue as JsonValue;
        } else if (isRefEncoded(descriptor)) {
          if (initValue != null) {
            let refId = (initValue as unknown as CoValue).$jazz?.id;
            if (!refId) {
              const coValue = instantiateRefEncodedWithInit(
                descriptor,
                initValue,
                owner,
              );
              refId = coValue.$jazz.id;
            }
            rawInit[key] = refId;
          }
        } else if ("encoded" in descriptor) {
          rawInit[key] = descriptor.encoded.encode(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            initValue as any,
          );
        }
      }

    return rawOwner.createMap(rawInit, null, "private", uniqueness);
  }

  /**
   * Declare a Record-like CoMap schema, by extending `CoMap.Record(...)` and passing the value schema using `co`. Keys are always `string`.
   *
   * @example
   * ```ts
   * import { coField, CoMap } from "jazz-tools";
   *
   * class ColorToFruitMap extends CoMap.Record(
   *  coField.ref(Fruit)
   * ) {}
   *
   * // assume we have map: ColorToFruitMap
   * // and strawberry: Fruit
   * map["red"] = strawberry;
   * ```
   *
   * @category Declaration
   */
  static Record<Value>(value: Value) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
    class RecordLikeCoMap extends CoMap {
      [ItemsSym] = value;
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
    interface RecordLikeCoMap extends Record<string, Value> {}

    return RecordLikeCoMap;
  }

  /**
   * Load a `CoMap` with a given ID, as a given account.
   *
   * `depth` specifies which (if any) fields that reference other CoValues to load as well before resolving.
   * The `DeeplyLoaded` return type guarantees that corresponding referenced CoValues are loaded to the specified depth.
   *
   * You can pass `[]` or `{}` for shallowly loading only this CoMap, or `{ fieldA: depthA, fieldB: depthB }` for recursively loading referenced CoValues.
   *
   * Check out the `load` methods on `CoMap`/`CoList`/`CoFeed`/`Group`/`Account` to see which depth structures are valid to nest.
   *
   * @example
   * ```ts
   * const person = await Person.load(
   *   "co_zdsMhHtfG6VNKt7RqPUPvUtN2Ax",
   *   { pet: {} }
   * );
   * ```
   *
   * @category Subscription & Loading
   *
   * @deprecated Use `co.map(...).load` instead.
   */
  static load<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: CoValueClass<M>,
    id: ID<M>,
    options?: {
      resolve?: RefsToResolveStrict<M, R>;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
    },
  ): Promise<Resolved<M, R> | null> {
    return loadCoValueWithoutMe(this, id, options);
  }

  /**
   * Load and subscribe to a `CoMap` with a given ID, as a given account.
   *
   * Automatically also subscribes to updates to all referenced/nested CoValues as soon as they are accessed in the listener.
   *
   * `depth` specifies which (if any) fields that reference other CoValues to load as well before calling `listener` for the first time.
   * The `DeeplyLoaded` return type guarantees that corresponding referenced CoValues are loaded to the specified depth.
   *
   * You can pass `[]` or `{}` for shallowly loading only this CoMap, or `{ fieldA: depthA, fieldB: depthB }` for recursively loading referenced CoValues.
   *
   * Check out the `load` methods on `CoMap`/`CoList`/`CoFeed`/`Group`/`Account` to see which depth structures are valid to nest.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * Also see the `useCoState` hook to reactively subscribe to a CoValue in a React component.
   *
   * @example
   * ```ts
   * const unsub = Person.subscribe(
   *   "co_zdsMhHtfG6VNKt7RqPUPvUtN2Ax",
   *   { pet: {} },
   *   (person) => console.log(person)
   * );
   * ```
   *
   * @category Subscription & Loading
   *
   * @deprecated Use `co.map(...).subscribe` instead.
   */
  static subscribe<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: CoValueClass<M>,
    id: ID<M>,
    listener: (value: Resolved<M, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: CoValueClass<M>,
    id: ID<M>,
    options: SubscribeListenerOptions<M, R>,
    listener: (value: Resolved<M, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<M extends CoMap, const R extends RefsToResolve<M>>(
    this: CoValueClass<M>,
    id: ID<M>,
    ...args: SubscribeRestArgs<M, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<M, R>(this, id, options, listener);
  }

  /** @deprecated Use `CoMap.upsertUnique` and `CoMap.loadUnique` instead. */
  static findUnique<M extends CoMap>(
    this: CoValueClass<M>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ) {
    return CoMap._findUnique(unique, ownerID, as);
  }

  /** @internal */
  static _findUnique<M extends CoMap>(
    this: CoValueClass<M>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ) {
    as ||= activeAccountContext.get();

    const header = {
      type: "comap" as const,
      ruleset: {
        type: "ownedByGroup" as const,
        group: ownerID as RawCoID,
      },
      meta: null,
      uniqueness: unique,
    };
    const crypto =
      as[TypeSym] === "Anonymous" ? as.node.crypto : as.$jazz.localNode.crypto;
    return cojsonInternals.idforHeader(header, crypto) as ID<M>;
  }

  /**
   * Given some data, updates an existing CoMap or initialises a new one if none exists.
   *
   * Note: This method respects resolve options, and thus can return `null` if the references cannot be resolved.
   *
   * @example
   * ```ts
   * const activeEvent = await Event.upsertUnique(
   *   sourceData.identifier,
   *   workspace.id,
   *   {
   *     title: sourceData.title,
   *     identifier: sourceData.identifier,
   *     external_id: sourceData._id,
   *   },
   *   workspace
   * );
   * ```
   *
   * @param options The options for creating or loading the CoMap. This includes the intended state of the CoMap, its unique identifier, its owner, and the references to resolve.
   * @returns Either an existing & modified CoMap, or a new initialised CoMap if none exists.
   * @category Subscription & Loading
   *
   * @deprecated Use `co.map(...).upsertUnique` instead.
   */
  static async upsertUnique<
    M extends CoMap,
    const R extends RefsToResolve<M> = true,
  >(
    this: CoValueClass<M>,
    options: {
      value: Simplify<CoMapInit_DEPRECATED<M>>;
      unique: CoValueUniqueness["uniqueness"];
      owner: Account | Group;
      resolve?: RefsToResolveStrict<M, R>;
    },
  ): Promise<Resolved<M, R> | null> {
    const mapId = CoMap._findUnique(
      options.unique,
      options.owner.$jazz.id,
      options.owner.$jazz.loadedAs,
    );
    let map: Resolved<M, R> | null = await loadCoValueWithoutMe(this, mapId, {
      ...options,
      loadAs: options.owner.$jazz.loadedAs,
      skipRetry: true,
    });
    if (!map) {
      const instance = new this();
      map = CoMap._createCoMap(instance, options.value, {
        owner: options.owner,
        unique: options.unique,
      }) as Resolved<M, R>;
    } else {
      (map as M).$jazz.applyDiff(
        options.value as unknown as Partial<CoMapInit<M>>,
      );
    }

    return await loadCoValueWithoutMe(this, mapId, {
      ...options,
      loadAs: options.owner.$jazz.loadedAs,
      skipRetry: true,
    });
  }

  /**
   * Loads a CoMap by its unique identifier and owner's ID.
   * @param unique The unique identifier of the CoMap to load.
   * @param ownerID The ID of the owner of the CoMap.
   * @param options Additional options for loading the CoMap.
   * @returns The loaded CoMap, or null if unavailable.
   *
   * @deprecated Use `co.map(...).loadUnique` instead.
   */
  static loadUnique<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: CoValueClass<M>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<M, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<M, R> | null> {
    return loadCoValueWithoutMe(
      this,
      CoMap._findUnique(unique, ownerID, options?.loadAs),
      { ...options, skipRetry: true },
    );
  }
}

/**
 * Contains CoMap Jazz methods that are part of the {@link CoMap.$jazz`} property.
 */
class CoMapJazzApi<M extends CoMap> extends CoValueJazzApi<M> {
  constructor(
    private coMap: M,
    private getRaw: () => RawCoMap,
  ) {
    super(coMap);
  }

  /**
   * The ID of this `CoMap`
   * @category Content
   */
  get id(): ID<M> {
    return this.raw.id;
  }

  get owner(): Group {
    return getCoValueOwner(this.coMap);
  }

  /**
   * Get a value from the CoMap.
   *
   * @example
   * ```ts
   * person.$jazz.get("name"); // => "John"
   * // This is equivalent to:
   * person.name; // => "John"
   * ```
   *
   * @param key The key to get
   * @returns The value for the key
   *
   * @category Content
   */
  get<K extends CoKeys<M>>(key: K): M[K] {
    const raw = this.raw.get(key);

    const descriptor = this.getDescriptor(key as string);

    if (!descriptor) {
      throw Error(`Cannot get unknown key ${key}`);
    }

    if (descriptor === "json") {
      return raw as M[K];
    } else if ("encoded" in descriptor) {
      return (
        raw === undefined ? undefined : descriptor.encoded.decode(raw)
      ) as M[K];
    } else if (isRefEncoded(descriptor)) {
      return (
        raw === undefined || raw === null
          ? undefined
          : accessChildByKey(this.coMap, raw as string, key)
      ) as M[K];
    }
    throw Error(`Cannot get unknown key ${key}`);
  }

  /**
   * Set a value on the CoMap
   *
   * @param key The key to set
   * @param value The value to set
   *
   * @category Content
   */
  set<K extends CoKeys<M>>(key: K, value: CoFieldInit<M[K]>): void {
    const descriptor = this.getDescriptor(key as string);

    if (!descriptor) {
      throw Error(`Cannot set unknown key ${key}`);
    }

    let refId = (value as CoValue)?.$jazz?.id;
    if (descriptor === "json") {
      this.raw.set(key, value as JsonValue | undefined);
    } else if ("encoded" in descriptor) {
      this.raw.set(key, descriptor.encoded.encode(value));
    } else if (isRefEncoded(descriptor)) {
      if (value === undefined) {
        if (!descriptor.optional) {
          throw Error(`Cannot set required reference ${key} to undefined`);
        }
        this.raw.set(key, null);
      } else {
        if (!refId) {
          const coValue = instantiateRefEncodedWithInit(
            descriptor,
            value,
            this.owner,
          );
          refId = coValue.$jazz.id;
        }
        this.raw.set(key, refId);
      }
    }
  }

  /**
   * Delete a value from a CoMap.
   *
   * For record-like CoMaps (created with `co.record`), any string key can be deleted.
   * For struct-like CoMaps (created with `co.map`), only optional properties can be deleted.
   *
   * @param key The key to delete
   *
   * @category Content
   */
  delete<
    K extends OptionalCoKeys<M> | (string extends keyof M ? string : never),
  >(key: K): void {
    this.raw.delete(key);
  }

  /**
   * Modify the `CoMap` to match another map.
   *
   * The new values are assigned to the CoMap, overwriting existing values
   * when the property already exists.
   *
   * @param newValues - The new values to apply to the CoMap. For collaborative values,
   * both CoValues and JSON values are supported.
   * @returns The modified CoMap.
   *
   * @category Content
   */
  applyDiff<N extends Partial<CoMapInit<M>>>(newValues: N): M {
    for (const key in newValues) {
      if (Object.prototype.hasOwnProperty.call(newValues, key)) {
        const tKey = key as keyof typeof newValues & keyof this;
        const descriptor = this.getDescriptor(key);

        if (!descriptor) continue;

        const newValue = newValues[tKey];
        const currentValue = this.get(tKey as any);

        if (descriptor === "json" || "encoded" in descriptor) {
          if (currentValue !== newValue) {
            this.set(tKey as any, newValue as CoFieldInit<M[keyof M]>);
          }
        } else if (isRefEncoded(descriptor)) {
          const currentId = (currentValue as CoValue | undefined)?.$jazz.id;
          let newId = (newValue as CoValue | undefined)?.$jazz?.id;
          if (currentId !== newId) {
            this.set(tKey as any, newValue as CoFieldInit<M[keyof M]>);
          }
        }
      }
    }
    return this.coMap;
  }

  /**
   * Given an already loaded `CoMap`, ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `CoMap.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<Map extends CoMap, const R extends RefsToResolve<Map>>(
    this: CoMapJazzApi<Map>,
    options: {
      resolve: RefsToResolveStrict<Map, R>;
    },
  ): Promise<Resolved<Map, R>> {
    return ensureCoValueLoaded(this.coMap, options);
  }

  /**
   * Given an already loaded `CoMap`, subscribe to updates to the `CoMap` and ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `CoMap.subscribe()`, but you don't need to pass the ID or the account to load as again.
   *
   * Returns an unsubscribe function that you should call when you no longer need updates.
   *
   * @category Subscription & Loading
   **/
  subscribe<Map extends CoMap, const R extends RefsToResolve<Map> = true>(
    this: CoMapJazzApi<Map>,
    listener: (value: Resolved<Map, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<Map extends CoMap, const R extends RefsToResolve<Map> = true>(
    this: CoMapJazzApi<Map>,
    options: { resolve?: RefsToResolveStrict<Map, R> },
    listener: (value: Resolved<Map, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<Map extends CoMap, const R extends RefsToResolve<Map>>(
    this: CoMapJazzApi<Map>,
    ...args: SubscribeRestArgs<Map, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue(this.coMap, options, listener);
  }

  /**
   * Wait for the `CoMap` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  async waitForSync(options?: { timeout?: number }): Promise<void> {
    await this.raw.core.waitForSync(options);
  }

  /**
   * Get the descriptor for a given key
   * @internal
   */
  getDescriptor(key: string): Schema | undefined {
    return this.schema?.[key] || this.schema?.[ItemsSym];
  }

  /**
   * If property `prop` is a `coField.ref(...)`, you can use `coMap.$jazz.refs.prop` to access
   * the `Ref` instead of the potentially loaded/null value.
   *
   * This allows you to always get the ID or load the value manually.
   *
   * @example
   * ```ts
   * person.$jazz.refs.pet.id; // => ID<Animal>
   * person.$jazz.refs.pet.value;
   * // => Animal | null
   * const pet = await person.$jazz.refs.pet.load();
   * ```
   *
   * @category Content
   **/
  get refs(): Simplify<
    {
      [Key in CoKeys<M> as NonNullable<M[Key]> extends CoValue
        ? Key
        : never]?: RefIfCoValue<M[Key]>;
    } & {
      [Key in CoKeys<M> as M[Key] extends undefined
        ? never
        : M[Key] extends CoValue
          ? Key
          : never]: RefIfCoValue<M[Key]>;
    }
  > {
    return makeRefs<CoKeys<this>>(
      this.coMap,
      (key) => this.raw.get(key as string) as unknown as ID<CoValue>,
      () => {
        const keys = this.raw.keys().filter((key) => {
          const descriptor = this.getDescriptor(key as string);
          return (
            descriptor && descriptor !== "json" && isRefEncoded(descriptor)
          );
        }) as CoKeys<this>[];

        return keys;
      },
      this.loadedAs,
      (key) => this.getDescriptor(key as string) as RefEncoded<CoValue>,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ) as any;
  }

  /**
   * Get the edits made to the CoMap.
   *
   * @category Collaboration
   */
  getEdits(): CoMapEdits<M> {
    const map = this.coMap;
    return new Proxy(
      {},
      {
        get(_target, key) {
          const rawEdit = map.$jazz.raw.lastEditAt(key as string);
          if (!rawEdit) return undefined;

          const descriptor = map.$jazz.getDescriptor(key as string);

          if (!descriptor) return undefined;

          return {
            ...getEditFromRaw(map, rawEdit, descriptor, key as string),
            get all() {
              return [...map.$jazz.raw.editsAt(key as string)].map((rawEdit) =>
                getEditFromRaw(map, rawEdit, descriptor, key as string),
              );
            },
          };
        },
        ownKeys(_target) {
          return map.$jazz.raw.keys();
        },
        getOwnPropertyDescriptor(target, key) {
          return {
            value: Reflect.get(target, key),
            writable: false,
            enumerable: true,
            configurable: true,
          };
        },
      },
    );
  }

  /** @internal */
  override get raw() {
    return this.getRaw();
  }

  /**
   * The timestamp of the creation time of the CoMap
   *
   * @category Content
   */
  get createdAt(): number {
    return this.raw.earliestTxMadeAt ?? Number.MAX_SAFE_INTEGER;
  }

  /**
   * The timestamp of the last updated time of the CoMap
   *
   * @category Content
   */
  get lastUpdatedAt(): number {
    return this.raw.latestTxMadeAt;
  }

  /** @internal */
  get schema(): CoMapFieldSchema {
    return (this.coMap.constructor as typeof CoMap)._schema;
  }
}

export type CoKeys<Map extends object> = Exclude<
  keyof Map & string,
  keyof CoMap
>;

/**
 * Extract keys of properties that are required
 */
export type RequiredCoKeys<Map extends object> = {
  [K in CoKeys<Map>]: undefined extends Map[K] ? never : K;
}[CoKeys<Map>];

/**
 * Extract keys of properties that can be undefined
 */
export type OptionalCoKeys<Map extends object> = {
  [K in CoKeys<Map>]: undefined extends Map[K] ? K : never;
}[CoKeys<Map>];

/**
 * Force required ref fields to be non nullable
 *
 * Considering that:
 * - Optional refs are typed as coField<InstanceType<CoValueClass> | null | undefined>
 * - Required refs are typed as coField<InstanceType<CoValueClass> | null>
 *
 * This type works in two steps:
 * - Remove the null from both types
 * - Then we check if the input type accepts undefined, if positive we put the null union back
 *
 * So the optional refs stays unchanged while we safely remove the null union
 * from required refs
 *
 * This way required refs can be marked as required in the CoMapInit while
 * staying a nullable property for value access.
 *
 * Example:
 *
 * const map = MyCoMap.create({
 *   requiredRef: NestedMap.create({}) // null is not valid here
 * })
 *
 * map.requiredRef // this value is still nullable
 */
type ForceRequiredRef<V> = V extends InstanceType<CoValueClass> | null
  ? NonNullable<V>
  : V extends InstanceType<CoValueClass> | undefined
    ? V | null
    : V;

export type CoMapInit_DEPRECATED<Map extends object> = PartialOnUndefined<{
  [Key in CoKeys<Map>]: ForceRequiredRef<Map[Key]>;
}>;

export type CoMapInit<Map extends object> = {
  [K in RequiredCoKeys<Map>]: CoFieldInit<Map[K]>;
} & {
  [K in OptionalCoKeys<Map>]?: CoFieldInit<Map[K]> | undefined;
};

// TODO: cache handlers per descriptor for performance?
const CoMapProxyHandler: ProxyHandler<CoMap> = {
  get(target, key, receiver) {
    if (key === "_schema") {
      return Reflect.get(target, key);
    } else if (key in target) {
      return Reflect.get(target, key, receiver);
    } else {
      if (typeof key !== "string") {
        return undefined;
      }

      const descriptor = target.$jazz.getDescriptor(key as string);

      if (!descriptor) {
        return undefined;
      }

      return target.$jazz.get(key as never);
    }
  },
  set(target, key, value, receiver) {
    if (
      typeof key === "string" &&
      typeof value === "object" &&
      value !== null &&
      SchemaInit in value
    ) {
      (target.constructor as typeof CoMap)._schema ||= {};
      (target.constructor as typeof CoMap)._schema[key] = value[SchemaInit];
      return true;
    }

    const descriptor = target.$jazz.getDescriptor(key as string);

    if (!descriptor) return false;

    if (typeof key === "string") {
      throw Error("Cannot update a CoMap directly. Use `$jazz.set` instead.");
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  defineProperty(target, key, attributes) {
    if (
      "value" in attributes &&
      typeof attributes.value === "object" &&
      SchemaInit in attributes.value
    ) {
      (target.constructor as typeof CoMap)._schema ||= {};
      (target.constructor as typeof CoMap)._schema[key as string] =
        attributes.value[SchemaInit];
      return true;
    } else {
      return Reflect.defineProperty(target, key, attributes);
    }
  },
  ownKeys(target) {
    const keys = Reflect.ownKeys(target).filter((k) => k !== ItemsSym);

    for (const key of target.$jazz.raw.keys()) {
      if (!keys.includes(key)) {
        keys.push(key);
      }
    }

    return keys;
  },
  getOwnPropertyDescriptor(target, key) {
    if (key in target) {
      return Reflect.getOwnPropertyDescriptor(target, key);
    } else {
      const descriptor = target.$jazz.getDescriptor(key as string);

      if (descriptor || key in target.$jazz.raw.latest) {
        return {
          enumerable: true,
          configurable: true,
          writable: true,
        };
      }
    }
  },
  has(target, key) {
    // The `has` trap can be called when defining properties during CoMap creation
    // when using the class-based syntax. In that case, $jazz may not yet be initialized,
    // as it's defined afterwards in the create method.
    const descriptor = target.$jazz?.getDescriptor(key as string);

    if (target.$jazz?.raw && typeof key === "string" && descriptor) {
      return target.$jazz.raw.get(key) !== undefined;
    } else {
      return Reflect.has(target, key);
    }
  },
  deleteProperty(target, key) {
    const descriptor = target.$jazz.getDescriptor(key as string);

    if (typeof key === "string" && descriptor) {
      throw Error(
        "Cannot delete a CoMap property directly. Use `$jazz.delete` instead.",
      );
    } else {
      return Reflect.deleteProperty(target, key);
    }
  },
};

RegisteredSchemas["CoMap"] = CoMap;

/** @internal */
function getEditFromRaw(
  target: CoMap,
  rawEdit: {
    by: RawAccountID | AgentID;
    tx: CojsonInternalTypes.TransactionID;
    at: Date;
    value?: JsonValue | undefined;
  },
  descriptor: Schema,
  key: string,
) {
  return {
    value:
      descriptor === "json"
        ? rawEdit.value
        : "encoded" in descriptor
          ? rawEdit.value === null || rawEdit.value === undefined
            ? rawEdit.value
            : descriptor.encoded.decode(rawEdit.value)
          : accessChildById(target, rawEdit.value as string, descriptor),
    ref:
      descriptor !== "json" && isRefEncoded(descriptor)
        ? new Ref(
            rawEdit.value as ID<CoValue>,
            target.$jazz.loadedAs,
            descriptor,
            target,
          )
        : undefined,
    get by() {
      return (
        rawEdit.by &&
        accessChildById(target, rawEdit.by, {
          ref: Account,
          optional: false,
        })
      );
    },
    madeAt: rawEdit.at,
    key,
  };
}

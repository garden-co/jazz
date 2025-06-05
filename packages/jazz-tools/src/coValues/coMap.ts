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
import type {
  AnonymousJazzAgent,
  AnyAccountSchema,
  CoValue,
  CoValueClass,
  Group,
  ID,
  InstanceOfSchema,
  RefEncoded,
  RefIfCoValue,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Schema,
  SubscribeListenerOptions,
  SubscribeRestArgs,
} from "../internal.js";
import {
  Account,
  CoValueBase,
  ItemsSym,
  Ref,
  RegisteredSchemas,
  SchemaInit,
  accessChildById,
  accessChildByKey,
  activeAccountContext,
  ensureCoValueLoaded,
  inspect,
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

export type Simplify<A> = {
  [K in keyof A]: A[K];
} extends infer B
  ? B
  : never;

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
  /**
   * The ID of this `CoMap`
   * @category Content */
  declare id: ID<this>;
  /** @category Type Helpers */
  declare _type: "CoMap";
  static {
    this.prototype._type = "CoMap";
  }
  /** @category Internals */
  declare _raw: RawCoMap;

  /** @internal */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static _schema: any;
  /** @internal */
  get _schema() {
    return (this.constructor as typeof CoMap)._schema as {
      [key: string]: Schema;
    } & { [ItemsSym]?: Schema };
  }

  /**
   * The timestamp of the creation time of the CoMap
   */
  get _createdAt() {
    return this._raw.earliestTxMadeAt ?? Number.MAX_SAFE_INTEGER;
  }

  /**
   * The timestamp of the last updated time of the CoMap
   */
  get _lastUpdatedAt() {
    return this._raw.latestTxMadeAt;
  }

  /**
   * If property `prop` is a `coField.ref(...)`, you can use `coMaps._refs.prop` to access
   * the `Ref` instead of the potentially loaded/null value.
   *
   * This allows you to always get the ID or load the value manually.
   *
   * @example
   * ```ts
   * person._refs.pet.id; // => ID<Animal>
   * person._refs.pet.value;
   * // => Animal | null
   * const pet = await person._refs.pet.load();
   * ```
   *
   * @category Content
   **/
  get _refs(): Simplify<
    {
      [Key in CoKeys<this> as NonNullable<this[Key]> extends CoValue
        ? Key
        : never]?: RefIfCoValue<this[Key]>;
    } & {
      [Key in CoKeys<this> as this[Key] extends undefined
        ? never
        : this[Key] extends CoValue
          ? Key
          : never]: RefIfCoValue<this[Key]>;
    }
  > {
    return makeRefs<CoKeys<this>>(
      this,
      (key) => this._raw.get(key as string) as unknown as ID<CoValue>,
      () => {
        const keys = this._raw.keys().filter((key) => {
          const descriptor = this.getDescriptor(key as string);
          return (
            descriptor && descriptor !== "json" && isRefEncoded(descriptor)
          );
        }) as CoKeys<this>[];

        return keys;
      },
      this._loadedAs,
      (key) => this.getDescriptor(key as string) as RefEncoded<CoValue>,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ) as any;
  }

  /** @internal */
  public getEditFromRaw(
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
              target._loadedAs,
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

  /** @category Collaboration */
  get _edits() {
    const map = this;
    return new Proxy(
      {},
      {
        get(_target, key) {
          const rawEdit = map._raw.lastEditAt(key as string);
          if (!rawEdit) return undefined;

          const descriptor = map.getDescriptor(key as string);

          if (!descriptor) return undefined;

          return {
            ...map.getEditFromRaw(map, rawEdit, descriptor, key as string),
            get all() {
              return [...map._raw.editsAt(key as string)].map((rawEdit) =>
                map.getEditFromRaw(map, rawEdit, descriptor, key as string),
              );
            },
          };
        },
        ownKeys(_target) {
          return map._raw.keys();
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
    ) as {
      [Key in CoKeys<this>]?: LastAndAllCoMapEdits<this[Key]>;
    };
  }

  /** @internal */
  constructor(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options: { fromRaw: RawCoMap } | undefined,
  ) {
    super();

    if (options) {
      if ("fromRaw" in options) {
        Object.defineProperties(this, {
          id: {
            value: options.fromRaw.id as unknown as ID<this>,
            enumerable: false,
          },
          _raw: { value: options.fromRaw, enumerable: false },
        });
      } else {
        throw new Error("Invalid CoMap constructor arguments");
      }
    }

    return new Proxy(this, CoMapProxyHandler as ProxyHandler<this>);
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
   **/
  static create<M extends CoMap>(
    this: CoValueClass<M>,
    init: Simplify<CoMapInit<M>>,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) {
    const instance = new this();

    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    const raw = instance.rawFromInit(init, owner, uniqueness);

    Object.defineProperties(instance, {
      id: {
        value: raw.id,
        enumerable: false,
      },
      _raw: { value: raw, enumerable: false },
    });

    return instance;
  }

  /**
   * Return a JSON representation of the `CoMap`
   * @category Content
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, processedValues?: ID<CoValue>[]): any {
    const result = {
      id: this.id,
      _type: this._type,
    } as Record<string, any>;

    for (const key of this._raw.keys()) {
      const tKey = key as CoKeys<this>;
      const descriptor = this.getDescriptor(tKey);

      if (!descriptor) {
        continue;
      }

      if (descriptor == "json" || "encoded" in descriptor) {
        result[key] = this._raw.get(key);
      } else if (isRefEncoded(descriptor)) {
        const id = this._raw.get(key) as ID<CoValue>;

        if (processedValues?.includes(id) || id === this.id) {
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
            this.id,
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
   * Create a new `RawCoMap` from an initialization object
   * @internal
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rawFromInit<Fields extends object = Record<string, any>>(
    init: Simplify<CoMapInit<Fields>> | undefined,
    owner: Account | Group,
    uniqueness?: CoValueUniqueness,
  ) {
    const rawOwner = owner._raw;

    const rawInit = {} as {
      [key in keyof Fields]: JsonValue | undefined;
    };

    if (init)
      for (const key of Object.keys(init) as (keyof Fields)[]) {
        const initValue = init[key as keyof typeof init];

        const descriptor = this.getDescriptor(key as string);

        if (!descriptor) {
          continue;
        }

        if (descriptor === "json") {
          rawInit[key] = initValue as JsonValue;
        } else if (isRefEncoded(descriptor)) {
          if (initValue) {
            rawInit[key] = (initValue as unknown as CoValue).id;
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

  getDescriptor(key: string) {
    return this._schema?.[key] || this._schema?.[ItemsSym];
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
   */
  static load<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: CoValueClass<M>,
    id: ID<M>,
    options?: {
      resolve?: RefsToResolveStrict<M, R>;
      loadAs?: Account | AnonymousJazzAgent;
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

  static findUnique<M extends CoMap>(
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
      as._type === "Anonymous" ? as.node.crypto : as._raw.core.node.crypto;
    return cojsonInternals.idforHeader(header, crypto) as ID<M>;
  }

  /**
   * Given an already loaded `CoMap`, ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `CoMap.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<M extends CoMap, const R extends RefsToResolve<M>>(
    this: M,
    options: { resolve: RefsToResolveStrict<M, R> },
  ): Promise<Resolved<M, R>> {
    return ensureCoValueLoaded(this, options);
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
  subscribe<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: M,
    listener: (value: Resolved<M, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<M extends CoMap, const R extends RefsToResolve<M> = true>(
    this: M,
    options: { resolve?: RefsToResolveStrict<M, R> },
    listener: (value: Resolved<M, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<M extends CoMap, const R extends RefsToResolve<M>>(
    this: M,
    ...args: SubscribeRestArgs<M, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToExistingCoValue<M, R>(this, options, listener);
  }

  applyDiff<N extends Partial<CoMapInit<this>>>(newValues: N) {
    for (const key in newValues) {
      if (Object.prototype.hasOwnProperty.call(newValues, key)) {
        const tKey = key as keyof typeof newValues & keyof this;
        const descriptor = this.getDescriptor(key);

        if (!descriptor) continue;

        const newValue = newValues[tKey];
        const currentValue = (this as unknown as N)[tKey];

        if (descriptor === "json" || "encoded" in descriptor) {
          if (currentValue !== newValue) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (this as any)[tKey] = newValue;
          }
        } else if (isRefEncoded(descriptor)) {
          const currentId = (currentValue as CoValue | undefined)?.id;
          const newId = (newValue as CoValue | undefined)?.id;
          if (currentId !== newId) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (this as any)[tKey] = newValue;
          }
        }
      }
    }
    return this;
  }

  /**
   * Wait for the `CoMap` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this._raw.core.waitForSync(options);
  }
}

export type CoKeys<Map extends object> = Exclude<
  keyof Map & string,
  keyof CoMap
>;

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

export type CoMapInit<Map extends object> = {
  [Key in CoKeys<Map> as undefined extends Map[Key]
    ? never
    : Key]: ForceRequiredRef<Map[Key]>;
} & {
  [Key in CoKeys<Map>]?: ForceRequiredRef<Map[Key]>;
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

      const descriptor = target.getDescriptor(key as string);

      if (!descriptor) {
        return undefined;
      }

      const raw = target._raw.get(key);

      if (descriptor === "json") {
        return raw;
      } else if ("encoded" in descriptor) {
        return raw === undefined ? undefined : descriptor.encoded.decode(raw);
      } else if (isRefEncoded(descriptor)) {
        return raw === undefined || raw === null
          ? undefined
          : accessChildByKey(target, raw as string, key);
      }
    }
  },
  set(target, key, value, receiver) {
    if (
      (typeof key === "string" || ItemsSym) &&
      typeof value === "object" &&
      value !== null &&
      SchemaInit in value
    ) {
      (target.constructor as typeof CoMap)._schema ||= {};
      (target.constructor as typeof CoMap)._schema[key] = value[SchemaInit];
      return true;
    }

    const descriptor = target.getDescriptor(key as string);

    if (!descriptor) return false;

    if (typeof key === "string") {
      if (descriptor === "json") {
        target._raw.set(key, value);
      } else if ("encoded" in descriptor) {
        target._raw.set(key, descriptor.encoded.encode(value));
      } else if (isRefEncoded(descriptor)) {
        if (value === undefined) {
          if (descriptor.optional) {
            target._raw.set(key, null);
          } else {
            throw new Error(
              `Cannot set required reference ${key} to undefined`,
            );
          }
        } else if (value?.id) {
          target._raw.set(key, value.id);
        } else {
          throw new Error(
            `Cannot set reference ${key} to a non-CoValue. Got ${value}`,
          );
        }
      }
      return true;
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

    for (const key of target._raw.keys()) {
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
      const descriptor = target.getDescriptor(key as string);

      if (descriptor || key in target._raw.latest) {
        return {
          enumerable: true,
          configurable: true,
          writable: true,
        };
      }
    }
  },
  has(target, key) {
    const descriptor = target.getDescriptor(key as string);

    if (target._raw && typeof key === "string" && descriptor) {
      return target._raw.get(key) !== undefined;
    } else {
      return Reflect.has(target, key);
    }
  },
  deleteProperty(target, key) {
    const descriptor = target.getDescriptor(key as string);

    if (typeof key === "string" && descriptor) {
      target._raw.delete(key);
      return true;
    } else {
      return Reflect.deleteProperty(target, key);
    }
  },
};

RegisteredSchemas["CoMap"] = CoMap;

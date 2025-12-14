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
  AsLoaded,
  LoadedAndRequired,
  CoFieldInit,
  CoValue,
  CoValueClass,
  getCoValueOwner,
  Group,
  ID,
  Settled,
  PartialOnUndefined,
  RefEncoded,
  RefIfCoValue,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  FieldDescriptor,
  Simplify,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  BranchDefinition,
  Account,
  CoValueBase,
  CoValueJazzApi,
  CoValueLoadingState,
  ItemsMarker,
  Ref,
  RegisteredSchemas,
  accessChildById,
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
} from "../internal.js";

export type CoMapEdit<V> = {
  value?: V;
  ref?: RefIfCoValue<V>;
  by: Account | null;
  madeAt: Date;
  key?: string;
};

export type LastAndAllCoMapEdits<V> = CoMapEdit<V> & { all: CoMapEdit<V>[] };

export type CoMapEdits<M extends CoMap> = {
  [Key in CoKeys<M>]?: LastAndAllCoMapEdits<M[Key]>;
};

export type CoMapFieldSchema = {
  [key: string]: FieldDescriptor;
} & { [ItemsMarker]?: FieldDescriptor };

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
  static fields: CoMapFieldSchema;

  /** @internal */
  constructor(fields: CoMapFieldSchema, raw: RawCoMap) {
    super();

    const proxy = new Proxy(this, CoMapProxyHandler as ProxyHandler<this>);

    Object.defineProperties(this, {
      $jazz: {
        value: new CoMapJazzApi(proxy, raw, fields),
        enumerable: false,
      },
    });

    return proxy;
  }

  static fromRaw<M extends CoValue>(this: CoValueClass<M>, raw: RawCoMap): M {
    return new this(
      (this as unknown as { fields: CoMapFieldSchema }).fields,
      raw,
    ) as M;
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
    this: CoValueClass<M> & { fields: CoMapFieldSchema },
    init: object,
    options?:
      | {
          owner?: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    const raw = CoMap.rawFromInit(this.fields, init, owner, uniqueness);

    return new this(this.fields, raw);
  }

  /**
   * Return a JSON representation of the `CoMap`
   * @category Content
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  toJSON(_key?: string, processedValues?: ID<CoValue>[]): any {
    const result = {
      $jazz: { id: this.$jazz.id },
    } as Record<string, any>;

    for (const key of this.$jazz.raw.keys()) {
      const tKey = key as CoKeys<this>;
      const descriptor = this.$jazz.getDescriptor(tKey);

      if (!descriptor) {
        continue;
      }

      if (descriptor.type == "json" || descriptor.type == "encoded") {
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
   * Create a new `RawCoMap` from an initialization object
   * @internal
   */
  static rawFromInit(
    fields: CoMapFieldSchema,
    init: object | undefined,
    owner: Group,
    uniqueness?: CoValueUniqueness,
  ) {
    const rawOwner = owner.$jazz.raw;

    const rawInit = {} as {
      [key: string]: JsonValue | undefined;
    };

    if (init)
      for (const key of Object.keys(init)) {
        const initValue = init[key as keyof typeof init];

        const descriptor = fields?.[key] || fields?.[ItemsMarker];

        if (!descriptor) {
          continue;
        }

        if (descriptor.type === "json") {
          rawInit[key] = initValue as JsonValue;
        } else if (isRefEncoded(descriptor)) {
          if (initValue != null) {
            let refId = (initValue as unknown as CoValue).$jazz?.id;
            if (!refId) {
              const newOwnerStrategy =
                descriptor.permissions?.newInlineOwnerStrategy;
              const onCreate = descriptor.permissions?.onCreate;
              const coValue = instantiateRefEncodedWithInit(
                descriptor,
                initValue,
                owner,
                newOwnerStrategy,
                onCreate,
              );
              refId = coValue.$jazz.id;
            }
            rawInit[key] = refId;
          }
        } else if (descriptor.type == "encoded") {
          rawInit[key] = descriptor.encode(
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
      [ItemsMarker] = value;
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
    interface RecordLikeCoMap extends Record<string, Value> {}

    return RecordLikeCoMap;
  }
}

/**
 * Contains CoMap Jazz methods that are part of the {@link CoMap.$jazz`} property.
 */
class CoMapJazzApi<M extends CoMap> extends CoValueJazzApi<M> {
  constructor(
    private coMap: M,
    public raw: RawCoMap,
    private fields: CoMapFieldSchema,
  ) {
    super(coMap);
  }

  get owner(): Group {
    return getCoValueOwner(this.coMap);
  }

  /**
   * Check if a key is defined in the CoMap.
   *
   * This check does not load the referenced value or validate permissions.
   *
   * @param key The key to check
   * @returns True if the key is defined, false otherwise
   * @category Content
   */
  has(key: CoKeys<M>): boolean {
    const entry = this.raw.getRaw(key);
    return entry?.change !== undefined && entry.change.op !== "del";
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
    if (descriptor.type === "json") {
      this.raw.set(key, value as JsonValue | undefined);
    } else if (descriptor.type == "encoded") {
      this.raw.set(key, descriptor.encode(value));
    } else if (isRefEncoded(descriptor)) {
      if (value === undefined) {
        if (!descriptor.optional) {
          throw Error(`Cannot set required reference ${key} to undefined`);
        }
        this.raw.set(key, null);
      } else {
        if (!refId) {
          const newOwnerStrategy =
            descriptor.permissions?.newInlineOwnerStrategy;
          const onCreate = descriptor.permissions?.onCreate;
          const coValue = instantiateRefEncodedWithInit(
            descriptor,
            value,
            this.owner,
            newOwnerStrategy,
            onCreate,
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
  delete(
    key: OptionalCoKeys<M> | (string extends keyof M ? string : never),
  ): void {
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
  applyDiff(newValues: Partial<CoMapInit<M>>): M {
    for (const key in newValues) {
      if (Object.prototype.hasOwnProperty.call(newValues, key)) {
        const tKey = key as keyof typeof newValues & keyof this;
        const descriptor = this.getDescriptor(key);

        if (!descriptor) continue;

        const newValue = newValues[tKey];
        const currentValue = this.coMap[tKey];

        if (descriptor.type === "json" || descriptor.type == "encoded") {
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
      unstable_branch?: BranchDefinition;
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
    options: {
      resolve?: RefsToResolveStrict<Map, R>;
      unstable_branch?: BranchDefinition;
    },
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
  getDescriptor(key: string): FieldDescriptor | undefined {
    return this.fields?.[key] || this.fields?.[ItemsMarker];
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
      [Key in CoKeys<M> as LoadedAndRequired<M[Key]> extends CoValue
        ? Key
        : never]?: RefIfCoValue<M[Key]>;
    } & {
      // Non-loaded CoValue refs (i.e. refs with type CoValue | null) are still required refs
      [Key in CoKeys<M> as AsLoaded<M[Key]> extends CoValue
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
            descriptor && descriptor.type !== "json" && isRefEncoded(descriptor)
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

export type CoMapInit<Map extends object> = {
  [K in RequiredCoKeys<Map>]: CoFieldInit<Map[K]>;
} & {
  [K in OptionalCoKeys<Map>]?: CoFieldInit<Map[K]> | undefined;
};

// TODO: cache handlers per descriptor for performance?
const CoMapProxyHandler: ProxyHandler<CoMap> = {
  get(target, key, receiver) {
    if (key in target) {
      return Reflect.get(target, key, receiver);
    } else {
      if (typeof key !== "string") {
        return undefined;
      }

      const descriptor = target.$jazz.getDescriptor(key as string);

      if (!descriptor) {
        return undefined;
      }

      const raw = target.$jazz.raw.get(key);

      if (descriptor.type === "json") {
        return raw;
      } else if (descriptor.type == "encoded") {
        return raw === undefined ? undefined : descriptor.decode(raw);
      } else if (isRefEncoded(descriptor)) {
        return raw === undefined || raw === null
          ? undefined
          : accessChildByKey(target, raw as string, key);
      }
    }
  },
  set(target, key, value, receiver) {
    if (typeof key === "string") {
      throw Error("Cannot update a CoMap directly. Use `$jazz.set` instead.");
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  ownKeys(target) {
    const keys = Reflect.ownKeys(target).filter((k) => k !== ItemsMarker);

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
  descriptor: FieldDescriptor,
  key: string,
) {
  return {
    value:
      descriptor.type === "json"
        ? rawEdit.value
        : descriptor.type == "encoded"
          ? rawEdit.value === null || rawEdit.value === undefined
            ? rawEdit.value
            : descriptor.decode(rawEdit.value)
          : accessChildById(target, rawEdit.value as string, descriptor),
    ref:
      descriptor.type !== "json" && isRefEncoded(descriptor)
        ? new Ref(
            rawEdit.value as ID<CoValue>,
            target.$jazz.loadedAs,
            descriptor,
            target,
          )
        : undefined,
    get by() {
      if (!rawEdit.by) return null;

      const account = accessChildById(target, rawEdit.by, {
        type: "ref",
        ref: Account,
        optional: false,
        sourceSchema: Account,
      }) as Account;

      if (!account.$isLoaded) return null;

      return account;
    },
    madeAt: rawEdit.at,
    key,
  };
}

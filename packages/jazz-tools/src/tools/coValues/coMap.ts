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
  CoreCoMapSchema,
  coAccountDefiner,
  AnyZodOrCoValueSchema,
  CoreCoValueSchema,
  MaybeLoaded,
  AnyZodSchema,
  CoMapKeys,
  ResolveQueryStrict,
  Loaded,
  ResolveQuery,
  CoreCoRecordSchema,
  CoreAccountSchema,
  SchemaAtKey,
} from "../internal.js";
import { TypeOfZodSchema } from "../implementation/zodSchema/typeConverters/TypeOfZodSchema.js";

export type CoMapEdit<S extends AnyZodOrCoValueSchema> = {
  value?: S extends CoreCoValueSchema
    ? MaybeLoaded<S>
    : S extends AnyZodSchema
      ? TypeOfZodSchema<S>
      : never;
  ref?: RefIfCoValue<S>;
  by: Loaded<CoreAccountSchema, true> | null;
  madeAt: Date;
  key?: string;
};

export type LastAndAllCoMapEdits<S extends AnyZodOrCoValueSchema> =
  CoMapEdit<S> & { all: CoMapEdit<S>[] };

export type CoMapEdits<M extends CoreCoMapSchema> = {
  [Key in CoMapKeys<M>]?: LastAndAllCoMapEdits<M["shape"][Key]>;
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
export class CoMap<S extends CoreCoMapSchema | CoreCoRecordSchema>
  extends CoValueBase
  implements CoValue
{
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
  declare $jazz: CoMapJazzApi<this, S>;

  /** @internal */
  static fields: CoMapFieldSchema;

  /** @internal */
  constructor(fields: CoMapFieldSchema, raw: RawCoMap, sourceSchema: S) {
    super();

    const proxy = new Proxy(
      this,
      CoMapProxyHandler as unknown as ProxyHandler<this>,
    );

    Object.defineProperties(this, {
      $jazz: {
        value: new CoMapJazzApi(proxy, raw, fields, sourceSchema),
        enumerable: false,
      },
    });

    return proxy;
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
      const tKey = key as CoMapKeys<S>;
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

        const ref = this[tKey as keyof this];

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
}

/**
 * Contains CoMap Jazz methods that are part of the {@link CoMap.$jazz`} property.
 */
class CoMapJazzApi<
  M extends CoMap<S>,
  S extends CoreCoMapSchema | CoreCoRecordSchema,
> extends CoValueJazzApi<M> {
  constructor(
    private coMap: M,
    public raw: RawCoMap,
    private fields: CoMapFieldSchema,
    public sourceSchema: S,
  ) {
    super(coMap);

    if (!this.sourceSchema) {
      throw new Error("sourceSchema is required");
    }
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
  has(key: CoMapKeys<S>): boolean {
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
  set<K extends CoMapKeys<S>>(
    key: K,
    value: CoFieldInit<SchemaAtKey<S, K>>,
  ): void {
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
    key: OptionalCoKeys<S> | (string extends keyof M ? string : never),
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
  applyDiff(newValues: Partial<CoMapInit<S>>): M {
    for (const key in newValues) {
      if (Object.prototype.hasOwnProperty.call(newValues, key)) {
        const tKey = key as keyof typeof newValues & keyof M;
        const descriptor = this.getDescriptor(key);

        if (!descriptor) continue;

        const newValue = newValues[tKey];
        const currentValue = this.coMap[tKey];

        if (descriptor.type === "json" || descriptor.type == "encoded") {
          if (currentValue !== newValue) {
            this.set(tKey as any, newValue as any);
          }
        } else if (isRefEncoded(descriptor)) {
          const currentId = (currentValue as CoValue | undefined)?.$jazz.id;
          let newId = (newValue as CoValue | undefined)?.$jazz?.id;
          if (currentId !== newId) {
            this.set(tKey as any, newValue as any);
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
  ensureLoaded<S extends CoreCoMapSchema, const R extends ResolveQuery<S>>(
    this: CoMapJazzApi<CoMap<S>, S>,
    options: {
      resolve: ResolveQueryStrict<S, R>;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<S, R>> {
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
  subscribe<Map extends CoMap<S>, const R extends ResolveQuery<S> = true>(
    this: CoMapJazzApi<Map, S>,
    listener: (value: Loaded<S, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<Map extends CoMap<S>, const R extends ResolveQuery<S> = true>(
    this: CoMapJazzApi<Map, S>,
    options: {
      resolve?: ResolveQueryStrict<S, R>;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: Loaded<S, R>, unsubscribe: () => void) => void,
  ): () => void;
  subscribe<Map extends CoMap<S>, const R extends ResolveQuery<S>>(
    this: CoMapJazzApi<Map, S>,
    ...args: SubscribeRestArgs<S, R>
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
      [Key in CoMapKeys<S> as SchemaAtKey<S, Key> extends CoreCoValueSchema
        ? Key
        : never]?: RefIfCoValue<SchemaAtKey<S, Key>>;
    } & {
      // Non-loaded CoValue refs (i.e. refs with type CoValue | null) are still required refs
      [Key in CoMapKeys<S> as SchemaAtKey<S, Key> extends CoreCoValueSchema
        ? Key
        : never]: RefIfCoValue<SchemaAtKey<S, Key>>;
    }
  > {
    return makeRefs<CoMapKeys<S>>(
      this.coMap,
      (key) => this.raw.get(key as string) as unknown as ID<CoValue>,
      () => {
        const keys = this.raw.keys().filter((key) => {
          const descriptor = this.getDescriptor(key as string);
          return (
            descriptor && descriptor.type !== "json" && isRefEncoded(descriptor)
          );
        }) as CoMapKeys<S>[];

        return keys;
      },
      this.loadedAs,
      (key) =>
        this.getDescriptor(key as string) as RefEncoded<CoreCoValueSchema>,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ) as any;
  }

  /**
   * Get the edits made to the CoMap.
   *
   * @category Collaboration
   */
  getEdits(): CoMapEdits<S> {
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

/**
 * Extract keys of properties that are required
 */
export type RequiredCoKeys<M extends CoreCoMapSchema | CoreCoRecordSchema> = {
  [K in CoMapKeys<M>]: undefined extends SchemaAtKey<M, K> ? never : K;
}[CoMapKeys<M>];

/**
 * Extract keys of properties that can be undefined
 */
export type OptionalCoKeys<M extends CoreCoMapSchema | CoreCoRecordSchema> = {
  [K in CoMapKeys<M>]: undefined extends SchemaAtKey<M, K> ? K : never;
}[CoMapKeys<M>];

export type CoMapInit<M extends CoreCoMapSchema | CoreCoRecordSchema> = {
  [K in RequiredCoKeys<M>]: CoFieldInit<SchemaAtKey<M, K>>;
} & {
  [K in OptionalCoKeys<M>]?: CoFieldInit<SchemaAtKey<M, K>> | undefined;
};

// TODO: cache handlers per descriptor for performance?
const CoMapProxyHandler: ProxyHandler<
  CoMap<CoreCoMapSchema | CoreCoRecordSchema>
> = {
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

/** @internal */
function getEditFromRaw(
  target: CoMap<CoreCoMapSchema | CoreCoRecordSchema>,
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
        optional: false,
        sourceSchema: coAccountDefiner(),
      });

      if (!account.$isLoaded) return null;

      return account;
    },
    madeAt: rawEdit.at,
    key,
  };
}

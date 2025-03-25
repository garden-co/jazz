import { CoValueUniqueness, JsonValue, RawAccount, RawCoMap } from "cojson";
import { ZodType, ZodTypeAny } from "zod";
import type { Account } from "../../coValues/account.js";
import type { Group } from "../../coValues/group.js";
import { RegisteredSchemas } from "../../coValues/registeredSchemas.js";
import { AnonymousJazzAgent, ID } from "../../internal.js";
import { coValuesCache } from "../../lib/cache.js";
import { LazySchema, isLazySchema } from "../coValue/lazy.js";
import { isOptional } from "../coValue/optional.js";
import {
  Loaded,
  MaybeLoaded,
  ResolveQuery,
  ResolveQueryStrict,
  Unloaded,
} from "../coValue/types.js";
import { getUnloadedState } from "../coValue/unloaded.js";
import { CoValueResolutionNode, ensureCoValueLoaded } from "../subscribe.js";
import {
  AnyCoMapSchema,
  CoMapInit,
  CoMapSchema,
  CoMapSchemaClass,
  CoMapSchemaKey,
  CoValueSchema,
} from "./schema.js";
import { getOwnerFromRawValue } from "./utils.js";

type Relations<D extends CoValueSchema> = D extends AnyCoMapSchema
  ? {
      [K in keyof D["shape"]]: D["shape"][K] extends AnyCoMapSchema
        ? D["shape"][K]
        : D["shape"][K] extends LazySchema<infer T extends CoValueSchema>
          ? T
          : never;
    }
  : "never";

type RelationsKeys<D extends CoValueSchema> = keyof Relations<D> &
  (string | number);

type ChildMap<D extends AnyCoMapSchema> = Map<
  RelationsKeys<D>,
  Loaded<any, any> | Unloaded<any> | undefined
>;

type PropertyType<
  D extends AnyCoMapSchema,
  K extends CoMapSchemaKey<D>,
> = CoMapInit<D>[K];

export type LoadedCoMapJazzProps<
  D extends AnyCoMapSchema,
  R extends ResolveQuery<D> = true,
> = {
  $jazzState: "loaded";
  $jazz: CoMapJazzApi<D, R>;
};

export class CoMapJazzApi<
  D extends AnyCoMapSchema,
  R extends ResolveQuery<D> = true,
> {
  raw: RawCoMap;
  schema: CoMapSchemaClass<D["shape"], D["record"], D["isOptional"]>;
  id: ID<D>;
  _resolutionNode: CoValueResolutionNode<D, R> | undefined;
  refs: ChildMap<D> = new Map();
  protected lastUpdateTx: number;
  declare _instance: Loaded<D, R>;

  constructor(
    schema: D,
    raw: RawCoMap,
    resolutionNode?: CoValueResolutionNode<D, R>,
  ) {
    this.schema = schema as CoMapSchemaClass<
      D["shape"],
      D["record"],
      D["isOptional"]
    >;
    this.raw = raw;
    this.lastUpdateTx = raw.totalProcessedTransactions;
    this.id = raw.id as unknown as ID<D>;
    this._resolutionNode = resolutionNode;
  }

  _setInstance(instance: LoadedCoMapJazzProps<D, R>) {
    this._instance = instance as unknown as Loaded<D, R>;
  }

  _fillRef<K extends RelationsKeys<D>>(
    key: K,
    value: Loaded<any, any> | Unloaded<any> | undefined,
  ) {
    const descriptor = this.schema.get(key);

    if (descriptor && isRelationRef(descriptor)) {
      this.refs.set(key, value);
      Object.defineProperty(this._instance, key, {
        value,
        writable: false,
        enumerable: true,
        configurable: true,
      });
    } else {
      throw new Error(`Field ${key} is not a reference`);
    }
  }

  set<K extends CoMapSchemaKey<D>>(key: K, value: PropertyType<D, K>) {
    // TODO: Shall we throw if the key doesn't match the Record key schema?
    const descriptor = this.schema.get(key);

    if (descriptor && isRelationRef(descriptor)) {
      if (!value) {
        this.refs.delete(key as RelationsKeys<D>);
      } else {
        if (!isCoValue(value)) {
          // To support inline CoMap creation on set
          value = getSchemaFromDescriptor(this.schema, key).create(
            value,
            this.owner,
          ) as PropertyType<D, K>;
        }

        this.refs.set(key as RelationsKeys<D>, value as Loaded<any, any>);
      }
    }

    setValue(this.raw, this.schema, key, value as JsonValue);

    return this.updated();
  }

  updated(refs?: ChildMap<D>): Loaded<D, R> {
    if (this.lastUpdateTx === this.raw.totalProcessedTransactions && !refs) {
      return this._instance;
    }

    return createCoMapFromRaw<D, R>(
      this.schema as D,
      this.raw,
      refs ?? this.refs,
      this._resolutionNode,
    );
  }

  /**
   * Given an already loaded `CoMapSchema`, ensure that the specified fields are loaded to the specified depth.
   *
   * Works like `CoMapSchema.load()`, but you don't need to pass the ID or the account to load as again.
   *
   * @category Subscription & Loading
   */
  ensureLoaded<O extends ResolveQuery<D>>(options: {
    resolve: ResolveQueryStrict<D, O>;
  }): Promise<Loaded<D, O>> {
    return ensureCoValueLoaded<D, R, O>(this._instance, {
      resolve: options.resolve,
    });
  }

  /**
   * Wait for the `CoMapSchema` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }

  get _loadedAs(): Account | AnonymousJazzAgent {
    const rawAccount = this.raw.core.node.account;

    if (rawAccount instanceof RawAccount) {
      return coValuesCache.get(rawAccount, () =>
        RegisteredSchemas["Account"].fromRaw(rawAccount),
      );
    }

    return new AnonymousJazzAgent(this.raw.core.node);
  }

  get owner(): Account | Group {
    return getOwnerFromRawValue(this.raw);
  }
}

export function createCoMap<D extends AnyCoMapSchema>(
  schema: D,
  init: CoMapInit<D>,
  owner: Account | Group,
  uniqueness?: CoValueUniqueness,
) {
  const { raw, refs } = createCoMapFromInit(
    init as any,
    owner,
    schema,
    uniqueness,
  );

  return createCoMapFromRaw<D, true>(schema, raw, refs);
}

export function createCoMapFromRaw<
  D extends AnyCoMapSchema,
  R extends ResolveQuery<D>,
>(
  schema: D,
  raw: RawCoMap,
  refs?: ChildMap<D>,
  resolutionNode?: CoValueResolutionNode<D, R>,
) {
  const instance = Object.create({
    $jazz: new CoMapJazzApi(schema, raw, resolutionNode),
  }) as LoadedCoMapJazzProps<D, R>;
  instance.$jazz._setInstance(instance);

  const fields = new Set(schema.keys());

  if (schema.record) {
    for (const key of raw.keys()) {
      fields.add(key);
    }
  }

  for (const key of fields) {
    const descriptor = schema.get(key);

    if (descriptor && isRelationRef(descriptor)) {
      const ref = refs?.get(key);

      if (ref) {
        instance.$jazz._fillRef(key as any, ref);
      } else {
        instance.$jazz._fillRef(
          key as any,
          getUnloadedState(
            getSchemaFromDescriptor(schema, key),
            raw.get(key) as ID<any>,
          ),
        );
      }
    } else {
      Object.defineProperty(instance, key, {
        value: getValue(raw, schema, key as CoMapSchemaKey<D>),
        writable: false,
        enumerable: true,
        configurable: true,
      });
    }
  }

  if (refs) {
    for (const [key, value] of refs.entries()) {
      instance.$jazz._fillRef(key as any, value);
    }
  }

  return instance as unknown as Loaded<D, R>;
}

function getValue<D extends AnyCoMapSchema>(
  raw: RawCoMap,
  schema: D,
  key: CoMapSchemaKey<D>,
) {
  const descriptor = schema.get(key);

  if (descriptor && typeof key === "string") {
    const value = raw.get(key);

    if (isRelationRef(descriptor)) {
      return undefined;
    } else {
      try {
        // TODO: If something fails on parse, we should navigate the history and get the last valid value
        return descriptor.parse(value);
      } catch (error) {
        throw new Error(
          `Failed to parse field ${key}: ${JSON.stringify(error)}`,
        );
      }
    }
  } else {
    return undefined;
  }
}

function setValue<D extends AnyCoMapSchema>(
  raw: RawCoMap,
  schema: D,
  key: CoMapSchemaKey<D>,
  value: JsonValue,
) {
  const descriptor = schema.get(key);

  if (descriptor && typeof key === "string") {
    if (isRelationRef(descriptor)) {
      if (value === null || value === undefined) {
        if (isOptional(descriptor)) {
          raw.set(key, undefined);
        } else {
          throw new Error(`Field ${key} is required`);
        }
      } else {
        if (value && typeof value === "object" && "$jazz" in value) {
          raw.set(
            key,
            (value as unknown as Loaded<CoMapSchema<{}>, true>).$jazz.id,
          );
        } else {
          throw new Error(`The value assigned to ${key} is not a reference`);
        }
      }
    } else {
      // TODO: Provide better parse errors with the field information
      try {
        raw.set(key, descriptor.parse(value));
      } catch (error) {
        throw new Error(
          `Failed to parse field ${key}: ${JSON.stringify(error)}`,
        );
      }
    }

    return true;
  }
}

function createCoMapFromInit<D extends AnyCoMapSchema>(
  init: CoMapInit<D> | undefined,
  owner: Account | Group,
  schema: D,
  uniqueness?: CoValueUniqueness,
) {
  const rawOwner = owner._raw;

  const rawInit = {} as Record<string, JsonValue | undefined>;

  const refs = new Map<string, MaybeLoaded<any>>();

  if (init) {
    const fields = new Set(schema.keys());

    if (schema.record) {
      for (const key of Object.keys(init)) {
        fields.add(key);
      }
    }

    for (const key of fields) {
      const initValue = init[key] as
        | Loaded<CoValueSchema>
        | CoMapInit<any>
        | undefined;

      const descriptor = schema.get(key);

      if (!descriptor) {
        continue;
      }

      if (isRelationRef(descriptor)) {
        if (initValue === undefined) {
          if (isOptional(descriptor)) {
            rawInit[key] = undefined;
          } else {
            throw new Error(`Field ${key} is required`);
          }
        } else {
          let instance: MaybeLoaded<CoValueSchema>;

          if ("$jazz" in initValue) {
            instance = initValue as MaybeLoaded<CoValueSchema>;
          } else {
            instance = getSchemaFromDescriptor(schema, key).create(
              initValue,
              owner,
            ) as MaybeLoaded<CoValueSchema>;
          }

          rawInit[key] = instance.$jazz.id;
          refs.set(key as string, instance);
        }
      } else {
        // TODO: Provide better parse errors with the field information
        try {
          rawInit[key] = descriptor.parse(initValue);
        } catch (error) {
          throw new Error(
            `Failed to parse field ${key}: ${JSON.stringify(error)}`,
          );
        }
      }
    }
  }

  const raw = rawOwner.createMap(rawInit, null, "private", uniqueness);

  return { raw, refs };
}

function getSchemaFromDescriptor<
  S extends AnyCoMapSchema,
  K extends CoMapSchemaKey<S>,
>(schema: S, key: K) {
  const descriptor = schema.get(key);

  if (descriptor && isRelationRef(descriptor)) {
    if (isLazySchema<any>(descriptor)) {
      return descriptor.lazySchema();
    } else {
      return descriptor;
    }
  } else {
    throw new Error(`Field ${String(key)} is not a reference`);
  }
}

export function isRelationRef(
  descriptor: AnyCoMapSchema | ZodTypeAny | LazySchema<any>,
): descriptor is AnyCoMapSchema | LazySchema<any> {
  return descriptor instanceof CoMapSchemaClass || isLazySchema(descriptor);
}

export function isCoValue(
  value: unknown,
): value is LoadedCoMapJazzProps<any, any> {
  return typeof value === "object" && value !== null && "$jazz" in value;
}

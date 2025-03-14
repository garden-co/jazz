import { CoValueUniqueness } from "cojson";
import { TypeOf, ZodString, ZodTypeAny, z } from "zod";
import { Account } from "../../coValues/account.js";
import { Group } from "../../coValues/group.js";
import { parseCoValueCreateOptions } from "../../internal.js";
import { addOptional } from "../coValue/optional.js";
import { SelfReference, markSelfReferenceAsOptional } from "../coValue/self.js";
import {
  IsDepthLimit,
  addQuestionMarks,
  flatten,
} from "../coValue/typeUtils.js";
import {
  Loaded,
  LoadedCoMap,
  ResolveQuery,
  ValidateQuery,
} from "../coValue/types.js";
import { CoMap, createCoMap } from "./instance.js";

export type CoMapField = AnyCoMapSchema | ZodTypeAny | SelfReference;
export type CoMapRecordDef = { key: ZodString; value: CoMapField };

export type CoMapSchemaShape = {
  [key: string]: CoMapField;
};

export type CoMapSchemaKey<S extends AnyCoMapSchema> =
  | keyof S["shape"]
  | CoMapRecordKey<S>;

export type CoMapRecordKey<S extends AnyCoMapSchema> =
  S["record"] extends CoMapRecordDef ? TypeOf<S["record"]["key"]> : never;

export type CoMapRecordFieldType<S extends AnyCoMapSchema> =
  S["record"] extends CoMapRecordDef ? S["record"]["value"] : never;

export type CoValueSchema = AnyCoMapSchema;

export type UnwrapRecordReference<S extends AnyCoMapSchema> =
  CoMapRecordFieldType<S> extends CoValueSchema
    ? CoMapRecordFieldType<S>
    : CoMapRecordFieldType<S> extends SelfReference
      ? CoMapSchemaToClass<S>
      : never;

export type UnwrapReference<
  S extends AnyCoMapSchema,
  K extends CoMapSchemaKey<S>,
> = S["shape"][K] extends CoValueSchema
  ? S["shape"][K]
  : S["shape"][K] extends SelfReference
    ? CoMapSchemaToClass<S>
    : never;

export type CoMapSchemaRelationsKeys<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends CoValueSchema | SelfReference
    ? K
    : never;
}[keyof S["shape"]];

export type CoMapSchemaStaticPropKeys<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny ? K : never;
}[keyof S["shape"]];

type CoMapChildSchemaInit<
  Field extends CoMapField,
  S extends AnyCoMapSchema,
  CurrentDepth extends number[],
> =
  | CoMapInit<S, CurrentDepth> // To accept inline init values
  | LoadedCoMap<S, ResolveQuery<S>> // To accept Schema.create or loaded values as input
  | addOptional<S> // Adds undefined if the schema is optional
  | markSelfReferenceAsOptional<Field>; // Self references are always optional

export type CoMapInit<
  S extends AnyCoMapSchema,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? {}
  : flatten<
      addQuestionMarks<{
        [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny
          ? TypeOf<S["shape"][K]>
          : UnwrapReference<S, K> extends AnyCoMapSchema
            ? CoMapChildSchemaInit<
                S["shape"][K],
                UnwrapReference<S, K>,
                [0, ...CurrentDepth]
              >
            : never;
      }>
    > &
      (S["record"] extends undefined
        ? unknown
        : {
            [K in CoMapRecordKey<S>]: CoMapRecordFieldType<S> extends ZodTypeAny
              ? TypeOf<CoMapRecordFieldType<S>>
              : UnwrapRecordReference<S> extends AnyCoMapSchema
                ? CoMapChildSchemaInit<
                    CoMapRecordFieldType<S>,
                    UnwrapRecordReference<S>,
                    [0, ...CurrentDepth]
                  >
                : never;
          });

export type CoMapInitStrict<
  S extends AnyCoMapSchema,
  I,
> = I extends CoMapInit<S> ? CoMapInit<S> : I;

/**
 * This is a simplified version of CoMapInit that only includes the keys that are defined in the schema.
 * It is used to build the resolve type for the create method without paying the compelxity cost of the full CoMapInit.
 */
type CoMapSimpleInit<
  S extends AnyCoMapSchema,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? {}
  : {
      [K in keyof S["shape"]]?: any;
    } & (S["record"] extends CoMapRecordDef
      ? {
          [K in CoMapRecordKey<S>]: any;
        }
      : unknown);

type CoMapChildInitToRelationsToResolve<
  ChildSchema,
  I,
  CurrentDepth extends number[],
> = ChildSchema extends AnyCoMapSchema
  ? I extends CoMap<ChildSchema, infer R> // If the init is a CoMap, return the resolve query
    ? R
    : I extends CoMapSimpleInit<ChildSchema>
      ? CoMapInitToRelationsToResolve<ChildSchema, I, CurrentDepth>
      : never
  : never;

export type CoMapInitToRelationsToResolve<
  S extends AnyCoMapSchema,
  I,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? true
  : I extends CoMapSimpleInit<S>
    ? ValidateQuery<
        S,
        {
          [K in keyof I &
            CoMapSchemaRelationsKeys<S>]: CoMapChildInitToRelationsToResolve<
            UnwrapReference<S, K>,
            I[K],
            [0, ...CurrentDepth]
          >;
        } & (S["record"] extends CoMapRecordDef
          ? {
              [K in keyof I &
                CoMapRecordKey<S>]: CoMapChildInitToRelationsToResolve<
                UnwrapRecordReference<S>,
                I[K],
                [0, ...CurrentDepth]
              >;
            }
          : unknown)
      >
    : true;

export interface CoMapSchema<
  S extends CoMapSchemaShape,
  R extends CoMapRecordDef | undefined = CoMapRecordDef | undefined,
  O extends boolean = boolean,
> {
  shape: S;
  record: R;
  isOptional: O;

  get(key: CoMapSchemaKey<CoMapSchema<S, R>>): CoMapField | undefined;
  optional(): CoMapSchema<S, R, true>;
  catchall<T extends CoMapField>(
    type: T,
  ): CoMapSchema<S, { key: ZodString; value: T }, O>;
  keys(): (keyof S & string)[];
}

export type CoMapSchemaToClass<D extends AnyCoMapSchema> =
  D extends AnyCoMapSchema
    ? D
    : CoMapSchema<D["shape"], D["record"], D["isOptional"]>;

export type AnyCoMapSchemaClass =
  | CoMapSchemaClass<any, undefined, true>
  | CoMapSchemaClass<any, CoMapRecordDef, false>
  | CoMapSchemaClass<any, undefined | CoMapRecordDef, true>;

export type AnyCoMapSchema =
  | CoMapSchema<any, undefined>
  | CoMapSchema<any, CoMapRecordDef>
  | CoMapSchema<any, undefined | CoMapRecordDef>
  | AnyCoMapSchemaClass;

export class CoMapSchemaClass<
  S extends CoMapSchemaShape,
  R extends CoMapRecordDef | undefined = CoMapRecordDef | undefined,
  O extends boolean = boolean,
> implements CoMapSchema<S, R, O>
{
  shape: S;
  record: R;
  isOptional: O;

  constructor(schema: S, record: R, isOptional: O) {
    this.shape = schema;
    this.record = record;
    this.isOptional = isOptional;
  }

  optional() {
    return new CoMapSchemaClass(this.shape, this.record, true);
  }

  get(key: CoMapSchemaKey<CoMapSchema<S, R>>): CoMapField | undefined {
    if (this.shape[key]) {
      return this.shape[key];
    }

    if (this.record) {
      if (this.record.key.safeParse(key).success) {
        return this.record.value;
      }
    }

    return undefined;
  }

  catchall<T extends CoMapField>(type: T) {
    const record = {
      key: z.string(),
      value: type,
    };

    return new CoMapSchemaClass(this.shape, record, this.isOptional);
  }

  keys() {
    return Object.keys(this.shape) as (keyof S & string)[];
  }

  create<I extends CoMapInit<CoMapSchema<S, R>>>(
    // Removing this extends check triggers "Expression produces a union type that is too complex to represent" error
    init: R extends undefined ? CoMapInitStrict<CoMapSchema<S, R>, I> : I,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ): Loaded<
    CoMapSchema<S, R, false>,
    CoMapInitToRelationsToResolve<CoMapSchema<S, R>, I>,
    "non-nullable" // We want the loaded type to reflect the init input as we know for sure if values are available or not
  > {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    return createCoMap(this as any, init as any, owner, uniqueness) as any;
  }
}

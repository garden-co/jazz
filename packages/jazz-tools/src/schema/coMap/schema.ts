import { CoValueUniqueness } from "cojson";
import { TypeOf, ZodString, ZodTypeAny, z } from "zod";
import { Account } from "../../coValues/account.js";
import { Group } from "../../coValues/group.js";
import { parseCoValueCreateOptions } from "../../internal.js";
import { LazySchema } from "../coValue/lazy.js";
import { addOptional } from "../coValue/optional.js";
import {
  IsDepthLimit,
  addQuestionMarks,
  flatten,
} from "../coValue/typeUtils.js";
import { Loaded, MaybeLoaded, ValidateQuery } from "../coValue/types.js";
import { LoadedCoMapJazzProps, createCoMap } from "./instance.js";

export type CoMapField = AnyCoMapSchema | ZodTypeAny | LazySchema<any>;
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
    : CoMapRecordFieldType<S> extends LazySchema<infer T>
      ? T extends CoValueSchema
        ? T
        : never
      : never;

export type UnwrapReference<
  S extends AnyCoMapSchema,
  K extends CoMapSchemaKey<S>,
> = S["shape"][K] extends CoValueSchema
  ? S["shape"][K]
  : S["shape"][K] extends LazySchema<infer T>
    ? T
    : never;

export type RefProps<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends CoValueSchema | LazySchema<any>
    ? K
    : never;
}[keyof S["shape"]];

export type PrimitiveProps<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny ? K : never;
}[keyof S["shape"]];

type CoMapChildSchemaInit<
  S extends AnyCoMapSchema,
  CurrentDepth extends number[],
> =
  | CoMapInit<S, CurrentDepth> // To accept inline init values
  | MaybeLoaded<S> // To accept Schema.create or loaded values as input
  | addOptional<S>; // Adds undefined if the schema is optional

export type CoMapInit<
  S extends AnyCoMapSchema,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? "TOO DEEP"
  : flatten<
      addQuestionMarks<{
        [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny
          ? TypeOf<S["shape"][K]>
          : UnwrapReference<S, K> extends AnyCoMapSchema
            ? CoMapChildSchemaInit<UnwrapReference<S, K>, [0, ...CurrentDepth]>
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
      : {});

type ReolveQueryForCoMapChild<
  ChildSchema,
  I,
  CurrentDepth extends number[],
> = ChildSchema extends AnyCoMapSchema
  ? I extends LoadedCoMapJazzProps<ChildSchema, infer R> // If the init is a CoMap, return the resolve query
    ? R
    : I extends CoMapSimpleInit<ChildSchema>
      ? ResolveQueryForCoMapInit<ChildSchema, I, CurrentDepth>
      : never
  : never;

export type ResolveQueryForCoMapInit<
  S extends AnyCoMapSchema,
  I,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? true
  : I extends CoMapSimpleInit<S>
    ? ValidateQuery<
        S,
        {
          [K in keyof I & RefProps<S>]: ReolveQueryForCoMapChild<
            UnwrapReference<S, K>,
            I[K],
            [0, ...CurrentDepth]
          >;
        } & (S["record"] extends CoMapRecordDef
          ? {
              [K in keyof I & CoMapRecordKey<S>]: ReolveQueryForCoMapChild<
                UnwrapRecordReference<S>,
                I[K],
                [0, ...CurrentDepth]
              >;
            }
          : {})
      >
    : true;

export type CoMapSchema<
  S extends CoMapSchemaShape,
  R extends CoMapRecordDef | undefined = CoMapRecordDef | undefined,
  O extends boolean = boolean,
> = {
  shape: S;
  record: R;
  isOptional: O;

  get(key: CoMapSchemaKey<CoMapSchema<S, R>>): CoMapField | undefined;
  optional(): CoMapSchema<S, R, true>;
  catchall<T extends CoMapField>(
    type: T,
  ): CoMapSchema<S, { key: ZodString; value: T }, O>;
  keys(): (keyof S & string)[];
};

export type CoValueClassToSchema<D extends CoValueSchema> =
  D extends AnyCoMapSchema ? CoMapClassToSchema<D> : never;

export type CoMapClassToSchema<D extends AnyCoMapSchema> = D extends {
  $isSchemaClass: true;
}
  ? CoMapSchema<D["shape"], D["record"], D["isOptional"]>
  : D;

export type CoValueSchemaToClass<D extends CoValueSchema> =
  D extends AnyCoMapSchema ? CoMapSchemaToClass<D> : never;

export type CoMapSchemaToClass<D extends AnyCoMapSchema> = D extends {
  $isSchemaClass: true;
}
  ? D
  : CoMapSchemaClass<D["shape"], D["record"], D["isOptional"]>;

export type AnyCoMapSchemaClass =
  | CoMapSchemaClass<any, undefined, boolean>
  | CoMapSchemaClass<any, CoMapRecordDef, boolean>
  | CoMapSchemaClass<any, undefined | CoMapRecordDef, boolean>;

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

  declare $isSchemaClass: true;

  constructor(schema: S, record: R, isOptional: O) {
    this.shape = schema;
    this.record = record;
    this.isOptional = isOptional;
  }

  optional(): CoMapSchemaClass<S, R, true> {
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
    ResolveQueryForCoMapInit<CoMapSchema<S, R>, I>,
    "non-nullable" // We want the loaded type to reflect the init input as we know for sure if values are available or not
  > {
    const { owner, uniqueness } = parseCoValueCreateOptions(options);
    return createCoMap(this as any, init as any, owner, uniqueness) as any;
  }
}

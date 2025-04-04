import { CoValueUniqueness } from "cojson";
import { TypeOf, ZodString, ZodTypeAny, z } from "zod";
import { Account } from "../../coValues/account.js";
import { Group } from "../../coValues/group.js";
import { parseCoValueCreateOptions } from "../../internal.js";
import { LazySchema } from "../coValue/lazy.js";
import {
  IsDepthLimit,
  ResolveQueryOf,
  SchemaOf,
  flatten,
  simplifyResolveQuery,
} from "../coValue/typeUtils.js";
import { Loaded } from "../coValue/types.js";
import { createCoMap } from "./instance.js";

export type CoMapField = CoValueSchema | ZodTypeAny | LazySchema<any>;
export type CoMapRecordDef = { key: ZodString; value: CoMapField };

export type CoMapSchemaShape = {
  [key: string]: CoMapField;
};

export type CoMapSchemaKey<S extends AnyCoMapSchema> =
  | keyof S["shape"]
  | CoMapRecordKey<S>;

export type CoMapRecordKey<S extends AnyCoMapSchema> =
  S["record"] extends CoMapRecordDef ? TypeOf<S["record"]["key"]> : never;

export type CoValueSchema = AnyCoMapSchema;

export type RefProps<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends { _schema: CoValueSchema }
    ? K
    : never;
}[keyof S["shape"]];

export type PrimitiveProps<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny ? K : never;
}[keyof S["shape"]];

type OptionalKeys<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny
    ? undefined extends TypeOf<S["shape"][K]>
      ? K
      : never
    : SchemaOf<S["shape"][K]> extends { isOptional: true }
      ? K
      : never;
}[keyof S["shape"]];

type RequiredKeys<S extends AnyCoMapSchema> = {
  [K in keyof S["shape"]]: S["shape"][K] extends ZodTypeAny
    ? undefined extends TypeOf<S["shape"][K]>
      ? never
      : K
    : SchemaOf<S["shape"][K]> extends { isOptional: true }
      ? never
      : K;
}[keyof S["shape"]];

export type CoMapInit<
  S extends AnyCoMapSchema,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? "TOO DEEP"
  : flatten<
      {
        [K in OptionalKeys<S>]?: S["shape"][K] extends ZodTypeAny
          ? TypeOf<S["shape"][K]>
          : CoMapInit<SchemaOf<S["shape"][K]>, [0, ...CurrentDepth]>;
      } & {
        [K in RequiredKeys<S>]: S["shape"][K] extends ZodTypeAny
          ? TypeOf<S["shape"][K]>
          : CoMapInit<SchemaOf<S["shape"][K]>, [0, ...CurrentDepth]>;
      } & {
        [K in keyof S["shape"]]?: unknown;
      }
    > &
      (S["record"] extends CoMapRecordDef
        ? {
            [K in CoMapRecordKey<S>]: S["record"]["value"] extends ZodTypeAny
              ? TypeOf<S["record"]["value"]>
              : S["record"]["value"] extends { _schema: CoValueSchema }
                ? CoMapInit<
                    SchemaOf<S["record"]["value"]>,
                    [0, ...CurrentDepth]
                  >
                : never;
          }
        : unknown);

export type CoMapInitStrict<
  S extends AnyCoMapSchema,
  I,
> = I extends CoMapInit<S> ? CoMapInit<S> : I;

type ReolveQueryForCoMapChild<
  ChildSchema,
  I,
  CurrentDepth extends number[],
> = ChildSchema extends AnyCoMapSchema
  ? I extends { $jazz: { _resolveQuery: any } }
    ? ResolveQueryOf<I>
    : ResolveQueryForCoMapInit<ChildSchema, I, CurrentDepth>
  : never;

export type ResolveQueryForCoMapInit<
  S extends AnyCoMapSchema,
  I,
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? true
  : I extends CoMapInit<S>
    ? simplifyResolveQuery<
        {
          [K in keyof I & RefProps<S>]: ReolveQueryForCoMapChild<
            SchemaOf<S["shape"][K]>,
            I[K],
            [0, ...CurrentDepth]
          >;
        } & (S["record"] extends { value: { _schema: CoValueSchema } }
          ? {
              [K in keyof I & CoMapRecordKey<S>]: ReolveQueryForCoMapChild<
                SchemaOf<S["record"]["value"]>,
                I[K],
                [0, ...CurrentDepth]
              >;
            }
          : unknown)
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

  _schema: CoMapSchema<S, R, O>;
  _ID: { shape: S; record: R }; // Used to identify the schema through the ID type

  get(key: CoMapSchemaKey<CoMapSchema<S, R>>): CoMapField | undefined;
  keys(): (keyof S & string)[];
};

export type CoValueSchemaToClass<D extends CoValueSchema> =
  D extends AnyCoMapSchema ? CoMapSchemaToClass<D> : never;

export type CoMapSchemaToClass<D extends AnyCoMapSchema> = D extends {
  _isSchemaClass: true;
}
  ? D
  : CoMapSchemaClass<D["shape"], D["record"], D["isOptional"]>;

export type AnyCoMapSchema = CoMapSchema<any, undefined | CoMapRecordDef>;

export class CoMapSchemaClass<
  S extends CoMapSchemaShape,
  R extends CoMapRecordDef | undefined = CoMapRecordDef | undefined,
  O extends boolean = boolean,
> implements CoMapSchema<S, R, O>
{
  shape: S;
  record: R;
  isOptional: O;

  declare _schema: CoMapSchema<S, R, O>;
  declare _optionalType: CoMapSchemaClass<S, R, true>;
  declare _isSchemaClass: true;
  declare _ID: { shape: S; record: R };

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

// TODO: add type tests for co.record

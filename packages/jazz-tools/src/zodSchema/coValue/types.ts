import type { CojsonInternalTypes } from "cojson";
import { TypeOf, ZodError, ZodTypeAny } from "zod";
import { IDMarker } from "../../internal.js";
import { LoadedCoMapJazzProps } from "../coMap/instance.js";
import {
  AnyCoMapSchema,
  CoMapRecordDef,
  CoMapRecordKey,
  CoMapSchema,
  CoValueSchema,
  CoValueSchemaToClass,
  PrimitiveProps,
  RefProps,
} from "../coMap/schema.js";
import {
  IsDepthLimit,
  SchemaOf,
  flatten,
  simplifyResolveQuery,
  validResolveKeys,
} from "./typeUtils.js";

export type ResolveQueryStrict<
  T extends CoValueSchema,
  V,
> = V extends ResolveQuery<T> ? ResolveQuery<T> : V;

export type ResolveQuery<
  S extends CoValueSchema,
  CurrentDepth extends number[] = [],
> =
  | true
  | (IsDepthLimit<CurrentDepth> extends true
      ? true
      : S extends AnyCoMapSchema
        ? simplifyResolveQuery<
            {
              [K in keyof S["shape"]]?: S["shape"][K] extends {
                _schema: CoValueSchema;
              }
                ? ResolveQuery<SchemaOf<S["shape"][K]>, [0, ...CurrentDepth]>
                : never;
            } & (S["record"] extends { value: { _schema: CoValueSchema } }
              ? {
                  [K in CoMapRecordKey<S>]?: ResolveQuery<
                    SchemaOf<S["record"]["value"]>,
                    [0, ...CurrentDepth]
                  >;
                } & {
                  $each?: ResolveQuery<
                    SchemaOf<S["record"]["value"]>,
                    [0, ...CurrentDepth]
                  >;
                }
              : unknown)
          >
        : true);

export type isQueryLeafNode<R> = R extends boolean | undefined
  ? true
  : keyof R extends never // R = {}
    ? true
    : false;

export type Loaded<
  S extends CoValueSchema,
  R = true,
  Options extends "nullable" | "non-nullable" = "nullable",
  CurrentDepth extends number[] = [],
> = R extends never
  ? never
  : S extends AnyCoMapSchema
    ? LoadedCoMap<
        CoMapSchema<S["shape"], S["record"], false>,
        R,
        Options,
        CurrentDepth
      >
    : never;

export type LoadedCoMap<
  S extends AnyCoMapSchema,
  R,
  Options extends "nullable" | "non-nullable" = "non-nullable",
  CurrentDepth extends number[] = [],
> = flatten<
  IsDepthLimit<CurrentDepth> extends true
    ? "You've reached the maximum depth of relations"
    : (S extends AnyCoMapSchema
        ? LoadedCoMapExplicitProps<S, R, Options, CurrentDepth> &
            (S extends CoMapSchema<any, CoMapRecordDef, boolean>
              ? LoadedCoMapRecordProps<S, R, Options, CurrentDepth>
              : {})
        : "Not a valid CoMapSchema") &
        (R extends ResolveQuery<S>
          ? LoadedCoMapJazzProps<S, R>
          : "Invalid query (CASE 2)" & { given: R; expected: ResolveQuery<S> })
>;

export type LoadedCoMapExplicitProps<
  S extends AnyCoMapSchema,
  R,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = LoadedCoMapExplicitPrimitiveProps<S> &
  LoadedCoMapExplicitRefProps<S, R, Options, CurrentDepth>;

export type LoadedCoMapExplicitPrimitiveProps<S extends AnyCoMapSchema> = {
  readonly [K in PrimitiveProps<S>]: S["shape"][K] extends ZodTypeAny
    ? TypeOf<S["shape"][K]>
    : never;
};

export type LoadedCoMapExplicitRefProps<
  S extends AnyCoMapSchema,
  R,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = {
  readonly [K in RefProps<S>]: S["shape"][K] extends {
    _schema: infer ChildSchema;
  }
    ? ChildSchema extends CoValueSchema
      ? isQueryLeafNode<R> extends true
        ? Options extends "non-nullable"
          ? ChildSchema extends { isOptional: true }
            ? undefined
            : MaybeLoaded<ChildSchema>
          : MaybeLoaded<ChildSchema> | addNullable<Options, ChildSchema>
        : R[K] extends ResolveQuery<ChildSchema>
          ?
              | Loaded<ChildSchema, R[K], Options, [0, ...CurrentDepth]>
              | addNullable<Options, ChildSchema>
          : "Invalid query (CASE 1)" & {
              given: R[K];
              parent: R;
              key: K;
              expected: ResolveQuery<ChildSchema>;
            }
      : "Invalid CoValue schema type"
    : "Invalid field value";
};

export type LoadedCoMapRecordProps<
  S extends CoMapSchema<any, CoMapRecordDef, boolean>,
  R,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = S["record"]["value"] extends { _schema: CoValueSchema }
  ? CoMapRecordExplicitlyQueriedProps<S, R, Options, CurrentDepth> &
      (R extends { $each: unknown }
        ? CoMapRecordQueriedByEachProps<S, R, R["$each"], Options, CurrentDepth>
        : {
            readonly [K in CoMapRecordKey<S>]?: MaybeLoaded<
              SchemaOf<S["record"]["value"]>
            >;
          })
  : S["record"]["value"] extends ZodTypeAny
    ? {
        // Filling the primitive record properties
        readonly [K in CoMapRecordKey<S>]: TypeOf<S["record"]["value"]>;
      }
    : "Not a valid record schema";

export type CoMapRecordExplicitlyQueriedProps<
  S extends CoMapSchema<any, CoMapRecordDef, boolean>,
  R,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = S["record"]["value"] extends { _schema: CoValueSchema }
  ? {
      // Filling the record relations directly resolved with the query
      readonly [K in Exclude<
        CoMapRecordKey<S> & validResolveKeys<R>,
        "$each"
      >]: R[K] extends ResolveQuery<SchemaOf<S["record"]["value"]>>
        ? isQueryLeafNode<R> extends true
          ?
              | Loaded<SchemaOf<S["record"]["value"]>>
              | addNullable<Options, SchemaOf<S["record"]["value"]>>
          :
              | Loaded<
                  SchemaOf<S["record"]["value"]>,
                  R[K],
                  Options,
                  [0, ...CurrentDepth]
                >
              | addNullable<Options, SchemaOf<S["record"]["value"]>>
        : K;
    }
  : {};

export type CoMapRecordQueriedByEachProps<
  S extends CoMapSchema<any, CoMapRecordDef, boolean>,
  R,
  EachQuery,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = S["record"]["value"] extends { _schema: CoValueSchema }
  ? {
      // Either fill the record relations or set them as null
      readonly [K in CoMapRecordKey<S>]: isQueryLeafNode<R> extends true
        ?
            | Loaded<SchemaOf<S["record"]["value"]>>
            | addNullable<Options, SchemaOf<S["record"]["value"]>>
        : EachQuery extends ResolveQuery<SchemaOf<S["record"]["value"]>>
          ?
              | Loaded<
                  SchemaOf<S["record"]["value"]>,
                  EachQuery,
                  Options,
                  [0, ...CurrentDepth]
                >
              | addNullable<Options, SchemaOf<S["record"]["value"]>>
          : "Invalid $each query";
    }
  : {};

export type UnloadedJazzAPI<D extends CoValueSchema> = flatten<{
  schema: CoValueSchemaToClass<D>;
  id: ID<D>;
  error?: ZodError;
}>;

export type Unloaded<D extends CoValueSchema> = flatten<{
  $jazzState: "unloaded" | "unauthorized" | "unavailable" | "validationError";
  $jazz: UnloadedJazzAPI<D>;
}>;

export type MaybeLoaded<
  D extends CoValueSchema,
  R extends ResolveQuery<D> = true,
> = Loaded<D, R> | Unloaded<D>;

export type addNullable<
  O extends "nullable" | "non-nullable",
  T,
> = O extends "nullable"
  ? T extends { isOptional: true }
    ? undefined
    : never
  : never;

export type ID<S extends CoValueSchema> = CojsonInternalTypes.RawCoID &
  IDMarker<S["_ID"]>;

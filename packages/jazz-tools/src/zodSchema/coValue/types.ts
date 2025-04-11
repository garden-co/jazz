import type { CojsonInternalTypes } from "cojson";
import { TypeOf, ZodError, ZodTypeAny } from "zod";
import { IDMarker } from "../../internal.js";
import { LoadedCoListJazzProps } from "../coList/instance.js";
import {
  AnyCoListSchema,
  CoListInit,
  CoListSchema,
  ResolveQueryForCoListInit,
} from "../coList/schema.js";
import { LoadedCoMapJazzProps } from "../coMap/instance.js";
import {
  AnyCoMapSchema,
  CoMapInit,
  CoMapRecordDef,
  CoMapRecordKey,
  CoMapSchema,
  CoValueSchemaToClass,
  PrimitiveProps,
  RefProps,
  ResolveQueryForCoMapInit,
} from "../coMap/schema.js";
import { CoValueSchema } from "../types.js";
import {
  IsDepthLimit,
  ResolveQueryOf,
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
      : S extends AnyCoListSchema
        ? { $each: ResolveQuery<S["items"], [0, ...CurrentDepth]> }
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

export type ReolveQueryForCoInitChild<
  ChildSchema,
  I,
  CurrentDepth extends number[],
> = ChildSchema extends AnyCoListSchema
  ? I extends { $jazz: { _resolveQuery: any } }
    ? ResolveQueryOf<I>
    : ResolveQueryForCoListInit<ChildSchema, I, CurrentDepth>
  : ChildSchema extends AnyCoMapSchema
    ? I extends { $jazz: { _resolveQuery: any } }
      ? ResolveQueryOf<I>
      : ResolveQueryForCoMapInit<ChildSchema, I, CurrentDepth>
    : never;

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
    : S extends AnyCoListSchema
      ? LoadedCoList<CoListSchema<S["items"], false>, R, Options, CurrentDepth>
      : never;

export type LoadedCoList<
  S extends AnyCoListSchema,
  R,
  Options extends "nullable" | "non-nullable" = "non-nullable",
  CurrentDepth extends number[] = [],
> = IsDepthLimit<CurrentDepth> extends true
  ? "You've reached the maximum depth of relations"
  : R extends ResolveQuery<S>
    ? LoadedCoListJazzProps<
        S,
        S["items"] extends ZodTypeAny
          ? TypeOf<S["items"]>
          :
              | (R extends { $each: unknown }
                  ? Loaded<
                      SchemaOf<S["items"]>,
                      R["$each"],
                      Options,
                      [0, ...CurrentDepth]
                    >
                  : MaybeLoaded<SchemaOf<S["items"]>>)
              | addNullable<"nullable", SchemaOf<S["items"]>>, // Values on lists are always nullable
        R
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
    : (LoadedCoMapExplicitProps<S, R, Options, CurrentDepth> &
        (S extends CoMapSchema<any, CoMapRecordDef, boolean>
          ? LoadedCoMapRecordProps<S, R, Options, CurrentDepth>
          : {})) &
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

export type CoValueInit<
  D extends CoValueSchema,
  CurrentDepth extends number[],
> = D extends AnyCoListSchema
  ? CoListInit<D, CurrentDepth>
  : D extends AnyCoMapSchema
    ? CoMapInit<D, CurrentDepth>
    : never;

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

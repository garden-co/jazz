import type { CojsonInternalTypes } from "cojson";
import { TypeOf, ZodTypeAny } from "zod";
import { IDMarker } from "../../internal.js";
import { LoadedCoMapJazzProps } from "../coMap/instance.js";
import {
  AnyCoMapSchema,
  CoMapClassToSchema,
  CoMapRecordDef,
  CoMapRecordKey,
  CoMapSchema,
  CoValueClassToSchema,
  CoValueSchema,
  CoValueSchemaToClass,
  PrimitiveProps,
  RefProps,
  UnwrapRecordReference,
  UnwrapReference,
} from "../coMap/schema.js";
import { IsDepthLimit, flatten, simplifyResolveQuery } from "./typeUtils.js";

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
              [K in RefProps<S>]?: UnwrapReference<S, K> extends CoValueSchema
                ? ResolveQuery<UnwrapReference<S, K>, [0, ...CurrentDepth]>
                : never;
            } & (S["record"] extends CoMapRecordDef
              ? UnwrapRecordReference<S> extends CoValueSchema
                ? {
                    [K in CoMapRecordKey<S>]?: ResolveQuery<
                      UnwrapRecordReference<S>,
                      [0, ...CurrentDepth]
                    >;
                  } & {
                    $each?: ResolveQuery<
                      UnwrapRecordReference<S>,
                      [0, ...CurrentDepth]
                    >;
                  }
                : unknown
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
    ? LoadedCoMap<CoMapClassToSchema<S>, R, Options, CurrentDepth>
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
  readonly [K in RefProps<S>]: UnwrapReference<S, K> extends infer ChildSchema
    ? ChildSchema extends AnyCoMapSchema
      ? isQueryLeafNode<R> extends true
        ? MaybeLoaded<ChildSchema> | addNullable<Options, ChildSchema>
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
> = S["record"]["value"] extends ZodTypeAny
  ? {
      // Filling the primitive record properties
      readonly [K in CoMapRecordKey<S>]: TypeOf<S["record"]["value"]>;
    }
  : CoMapRecordExplicitlyQueriedProps<S, R, Options, CurrentDepth> &
      (R extends { $each: infer EachQuery }
        ? CoMapRecordQueriedByEachProps<S, R, EachQuery, Options, CurrentDepth>
        : {
            // Filling the primitive record properties
            readonly [K in CoMapRecordKey<S>]?: MaybeLoaded<
              UnwrapRecordReference<S>
            >;
          });

export type CoMapRecordExplicitlyQueriedProps<
  S extends CoMapSchema<any, CoMapRecordDef, boolean>,
  R,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = {
  // Filling the record relations directly resolved with the query
  readonly [K in Exclude<
    CoMapRecordKey<S> & keyof R,
    "$each"
  >]: R[K] extends ResolveQuery<UnwrapRecordReference<S>>
    ? isQueryLeafNode<R> extends true
      ?
          | MaybeLoaded<UnwrapRecordReference<S>>
          | addNullable<Options, UnwrapRecordReference<S>>
      :
          | Loaded<
              UnwrapRecordReference<S>,
              R[K],
              Options,
              [0, ...CurrentDepth]
            >
          | addNullable<Options, UnwrapRecordReference<S>>
    : "Not a valid reference key for the schema";
};

export type CoMapRecordQueriedByEachProps<
  S extends CoMapSchema<any, CoMapRecordDef, boolean>,
  R,
  EachQuery,
  Options extends "nullable" | "non-nullable",
  CurrentDepth extends number[],
> = {
  // Either fill the record relations or set them as null
  readonly [K in CoMapRecordKey<S>]: isQueryLeafNode<R> extends true
    ?
        | MaybeLoaded<UnwrapRecordReference<S>>
        | addNullable<Options, UnwrapRecordReference<S>>
    : EachQuery extends ResolveQuery<UnwrapRecordReference<S>>
      ?
          | Loaded<
              UnwrapRecordReference<S>,
              EachQuery,
              Options,
              [0, ...CurrentDepth]
            >
          | addNullable<Options, { isOptional: true }>
      : "Invalid $each query";
};

export type UnloadedJazzAPI<D extends CoValueSchema> = flatten<{
  schema: CoValueSchemaToClass<D>;
  id: ID<D>;
}>;

export type Unloaded<D extends CoValueSchema> = flatten<{
  $jazzState: "unloaded" | "unauthorized" | "unavailable";
  $jazz: UnloadedJazzAPI<D>;
}>;

export type MaybeLoaded<
  D extends CoValueSchema,
  R extends ResolveQuery<D> = true,
> = Loaded<D, R> | Unloaded<D>;

export type UnwrapZodType<T, O> = T extends ZodTypeAny ? TypeOf<T> : O;

export type ValidateQuery<
  D extends CoValueSchema,
  I,
> = I extends ResolveQuery<D> ? simplifyResolveQuery<I> : true; // TODO: this seems dangerous?

export type addNullable<
  O extends "nullable" | "non-nullable",
  T,
> = O extends "nullable"
  ? T extends { isOptional: true }
    ? undefined
    : never
  : never;

export type ID<D extends CoValueSchema> = CojsonInternalTypes.RawCoID &
  IDMarker<
    D extends AnyCoMapSchema
      ? {
          shape: D["shape"];
          record: D["record"];
        }
      : never
  >;

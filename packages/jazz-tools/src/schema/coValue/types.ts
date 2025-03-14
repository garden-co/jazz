import { TypeOf, ZodTypeAny } from "zod";
import { CoMap } from "../coMap/instance.js";
import {
  AnyCoMapSchema,
  CoMapRecordDef,
  CoMapRecordKey,
  CoMapSchemaRelationsKeys,
  CoMapSchemaStaticPropKeys,
  CoMapSchemaToClass,
  CoValueSchema,
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
              [K in CoMapSchemaRelationsKeys<S>]?: UnwrapReference<
                S,
                K
              > extends CoValueSchema
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
    ? LoadedCoMap<CoMapSchemaToClass<S>, R, Options, CurrentDepth>
    : never;

export type LoadedCoMap<
  S extends AnyCoMapSchema,
  R,
  Options extends "nullable" | "non-nullable" = "non-nullable",
  CurrentDepth extends number[] = [],
> = flatten<
  (S extends AnyCoMapSchema
    ? {
        readonly [K in CoMapSchemaStaticPropKeys<S>]: S["shape"][K] extends ZodTypeAny
          ? TypeOf<S["shape"][K]>
          : never;
      } & {
        readonly [K in CoMapSchemaRelationsKeys<S>]: UnwrapReference<
          S,
          K
        > extends infer ChildSchema
          ? ChildSchema extends AnyCoMapSchema
            ? R[K] extends ResolveQuery<ChildSchema>
              ? IsDepthLimit<CurrentDepth> & isQueryLeafNode<R> extends false
                ?
                    | Loaded<ChildSchema, R[K], Options, [0, ...CurrentDepth]>
                    | addNullable<Options, S["shape"][K]>
                : null
              : null
            : null
          : never;
      } & (S["record"] extends CoMapRecordDef
          ? S["record"]["value"] extends ZodTypeAny
            ? {
                // Filling the record properties
                readonly [K in CoMapRecordKey<S>]: TypeOf<S["record"]["value"]>;
              }
            : {
                // Filling the record relations directly resolved with the query
                readonly [K in Exclude<
                  CoMapRecordKey<S> & keyof R,
                  "$each"
                >]: R[K] extends ResolveQuery<UnwrapRecordReference<S>>
                  ? IsDepthLimit<CurrentDepth> &
                      isQueryLeafNode<R> extends false
                    ?
                        | Loaded<
                            UnwrapRecordReference<S>,
                            R[K],
                            Options,
                            [0, ...CurrentDepth]
                          >
                        | addNullable<Options, { isOptional: true }>
                    : null
                  : null;
              } & {
                // Either fill the record relations or set them as null
                readonly [K in CoMapRecordKey<S>]: R extends {
                  $each: ResolveQuery<UnwrapRecordReference<S>>;
                }
                  ? IsDepthLimit<CurrentDepth> &
                      isQueryLeafNode<R> extends false
                    ?
                        | Loaded<
                            UnwrapRecordReference<S>,
                            R["$each"],
                            Options,
                            [0, ...CurrentDepth]
                          >
                        | addNullable<Options, { isOptional: true }>
                    : null
                  : null;
              }
          : unknown)
    : never) &
    (R extends ResolveQuery<S> ? CoMap<S, R> : unknown)
>;

export type UnwrapZodType<T, O> = T extends ZodTypeAny ? TypeOf<T> : O;

export type ValidateQuery<
  D extends CoValueSchema,
  I,
> = I extends ResolveQuery<D> ? simplifyResolveQuery<I> : true;

export type addNullable<
  O extends "nullable" | "non-nullable",
  T,
> = O extends "nullable"
  ? T extends { isOptional: true }
    ? null | undefined
    : never
  : never;

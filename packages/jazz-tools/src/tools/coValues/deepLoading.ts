import { SessionID } from "cojson";
import {
  CoreAccountSchema,
  CoreCoDiscriminatedUnionSchema,
  CoreCoFeedSchema,
  CoreCoListSchema,
  CoreCoMapSchema,
  CoreCoRecordSchema,
  CoreCoVectorSchema,
  CoreFileStreamSchema,
  CorePlainTextSchema,
  CoreCoValueSchema,
  CoValueLoadingState,
  ItemsMarker,
  TypeSym,
  CoList,
  CoDiscriminatedUnionSchema,
  CoreGroupSchema,
  CoreRichTextSchema,
  CoRichText,
  CoPlainText,
  CoVector,
  Group,
  CoValueBase,
  AnyZodOrCoValueSchema,
  AnyZodSchema,
  Simplify,
} from "../internal.js";
import { type Account } from "./account.js";
import { CoFeed, CoFeedEntry, FileStream } from "./coFeed.js";
import { CoMap } from "./coMap.js";
import { type CoValue, type ID } from "./interfaces.js";
import { z } from "../implementation/zodSchema/zodReExport.js";
import { TypeOfZodSchema } from "../implementation/zodSchema/typeConverters/TypeOfZodSchema.js";
import { CoreCoOptionalSchema } from "../implementation/zodSchema/schemaTypes/CoOptionalSchema.js";

/**
 * Returns a boolean for whether the given type is a union.
 *
 * Taken from https://github.com/sindresorhus/type-fest/blob/main/source/is-union.d.ts
 */
type IsUnion<T, U = T> = (
  [T] extends [never]
    ? false
    : T extends any
      ? [U] extends [T]
        ? false
        : true
      : never
) extends infer Result
  ? boolean extends Result
    ? true
    : Result
  : never;

/**
 * A CoValue that may or may not be loaded.
 */
export type MaybeLoaded<T extends CoreCoValueSchema> =
  | Loaded<T, ResolveQuery<T>>
  | NotLoaded<T, ResolveQuery<T>>
  | Inaccessible<T>;

/**
 * A CoValue that is either successfully loaded or that could not be loaded.
 */
export type Settled<
  T extends CoreCoValueSchema,
  R extends ResolveQuery<T> = true,
> = Loaded<T, R> | Inaccessible<T>;

/**
 * A CoValue that is not loaded.
 */
export type NotLoaded<T, R = true> = {
  $jazz: {
    id: ID<T>;
    loadingState:
      | typeof CoValueLoadingState.LOADING
      | typeof CoValueLoadingState.UNAVAILABLE
      | typeof CoValueLoadingState.UNAUTHORIZED;
  };
  $isLoaded: false;
};

/**
 * A CoValue that is being loaded
 */
export type Loading<T> = {
  $jazz: {
    id: ID<T>;
    loadingState: typeof CoValueLoadingState.LOADING;
  };
  $isLoaded: false;
};

/**
 * A CoValue that could not be loaded
 */
export type Inaccessible<T> = {
  $jazz: {
    id: ID<T>;
    loadingState:
      | typeof CoValueLoadingState.UNAVAILABLE
      | typeof CoValueLoadingState.UNAUTHORIZED;
  };
  $isLoaded: false;
};

/**
 * Narrows a maybe-loaded, optional CoValue to a loaded and required CoValue.
 */
export type LoadedAndRequired<
  T extends CoreCoValueSchema,
  R extends ResolveQuery<T>,
> = Exclude<Loaded<T, R>, NotLoaded<T, R> | undefined>;

/**
 * Narrows a maybe-loaded, optional CoValue to a loaded and optional CoValue
 */
export type AsLoaded<
  T extends CoreCoValueSchema,
  R extends ResolveQuery<T>,
> = Exclude<Loaded<T, R>, NotLoaded<T, R>>;

/**
 * By default, if a nested CoValue is not loaded, the parent CoValue will not be loaded either.
 * When `$onError: "catch"` is used, the parent CoValue will always be loaded, and an {@link NotLoaded}
 * value will be returned for the nested CoValue if it cannot be loaded.
 *
 * Use `$onError` to handle cases where some data you have requested is inaccessible,
 * similar to a `try...catch` block in your query.
 */
type OnError = { $onError?: "catch" };

export type ResolveQuery<
  S extends CoreCoValueSchema,
  Dep extends number[] = [],
  Lim extends number = 10,
> =
  | boolean
  | (Lim extends Dep["length"]
      ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
        any
      : S extends CoreCoListSchema
        ? S["element"] extends CoreCoValueSchema
          ?
              | ({
                  $each?: ResolveQuery<S["element"], [0, ...Dep], Lim>;
                } & OnError)
              | boolean
          : OnError | boolean
        : S extends CoreCoMapSchema | CoreAccountSchema
          ?
              | ({
                  [Key in keyof S["shape"] &
                    string as S["shape"][Key] extends CoreCoValueSchema
                    ? Key
                    : never]?: ResolveQuery<S["shape"][Key], [0, ...Dep], Lim>;
                } & OnError)
              | (S["catchAll"] extends CoreCoValueSchema
                  ? {
                      $each: ResolveQuery<S["catchAll"], [0, ...Dep], Lim>;
                    } & OnError
                  : never)
          : S extends CoreCoRecordSchema
            ? S["valueType"] extends CoreCoValueSchema
              ? {
                  $each: ResolveQuery<S["valueType"], [0, ...Dep], Lim>;
                } & OnError
              : OnError | boolean
            : S extends CoreCoFeedSchema
              ? S["element"] extends CoreCoValueSchema
                ? {
                    $each: ResolveQuery<S["element"], [0, ...Dep], Lim>;
                  } & OnError
                : OnError | boolean
              : S extends
                    | CorePlainTextSchema
                    | CoreFileStreamSchema
                    | CoreCoVectorSchema
                ? boolean | OnError
                : S extends CoreCoDiscriminatedUnionSchema<infer Options>
                  ? ResolveQuery<Options[number], [0, ...Dep], Lim>
                  : boolean);

export type ResolveQueryStrict<
  T extends CoreCoValueSchema,
  R extends ResolveQuery<T>,
> = [R] extends [ResolveQuery<T>] ? ResolveQuery<T> : R;

// TODO: remove/inline
export type SchemaResolveQuery<T extends CoreCoValueSchema> = T["resolveQuery"];

export type Loaded<
  T extends CoreCoValueSchema,
  R extends ResolveQuery<T> = SchemaResolveQuery<T>,
> = Resolved<T, R>;

/**
 * If the resolve query contains `$onError: "catch"`, we return a not loaded value for this nested CoValue.
 * Otherwise, the whole load operation returns a not-loaded value.
 */
type OnErrorResolvedValue<S, Depth> = Depth extends { $onError: "catch" }
  ? NotLoaded<S>
  : never;

export type Resolved<S, R> = S extends CoreCoListSchema
  ? R extends ResolveQuery<S>
    ? CoList<S, R>
    : never
  : S extends CoreCoMapSchema | CoreCoRecordSchema
    ? R extends ResolveQuery<S>
      ? Simplify<ResolvedFields<S, R>> & CoMap<S, R>
      : never
    : S extends CoreAccountSchema
      ? Simplify<ResolvedFields<S, R>> & Account<S>
      : S extends CoreCoFeedSchema
        ? R extends ResolveQuery<S>
          ? CoFeed<S, R>
          : never
        : S extends CoreCoOptionalSchema<infer Inner>
          ? Resolved<Inner, R> // TODO | undefined
          : S extends CoDiscriminatedUnionSchema<infer Options>
            ? Resolved<Options[number], R>
            : S extends CoreGroupSchema
              ? Group
              : S extends CoreRichTextSchema
                ? CoRichText
                : S extends CorePlainTextSchema
                  ? CoPlainText
                  : S extends CoreFileStreamSchema
                    ? FileStream
                    : S extends CoreCoVectorSchema
                      ? CoVector
                      : CoValueBase & { $noMatchFor: S; $r: R };

export type PrimitiveOrLoaded<
  S extends AnyZodOrCoValueSchema,
  R extends any,
> = S extends CoreCoValueSchema
  ? Resolved<S, R>
  : S extends AnyZodSchema
    ? TypeOfZodSchema<S>
    : never;

export type PrimitiveOrInaccessible<S extends AnyZodOrCoValueSchema> =
  S extends CoreCoValueSchema
    ? Inaccessible<S>
    : S extends AnyZodSchema
      ? TypeOfZodSchema<S>
      : never;

export type PrimitiveOrMaybeLoaded<S extends AnyZodOrCoValueSchema> =
  S extends CoreCoValueSchema
    ? MaybeLoaded<S>
    : S extends AnyZodSchema
      ? TypeOfZodSchema<S>
      : never;

export type ResolvedElement<
  S extends CoreCoListSchema | CoreCoFeedSchema,
  R extends ResolveQuery<S>,
> = S["element"] extends CoreCoValueSchema
  ? R extends { $each: infer ElemR }
    ? Resolved<S["element"], ElemR>
    : MaybeLoaded<S["element"]>
  : PrimitiveOrLoaded<S, R>;

type ResolvedFields<
  S extends CoreCoMapSchema | CoreCoRecordSchema | CoreAccountSchema,
  R,
> = {
  readonly [key in CoMapKeys<S>]: SchemaAtKey<
    S,
    key
  > extends infer SAtKey extends CoreCoValueSchema
    ? key extends Exclude<keyof R, "$onError"> // is key also in resolve query?
      ? Resolved<SAtKey, R[key]> | OnErrorResolvedValue<SAtKey, R[key]>
      : PrimitiveOrMaybeLoaded<SAtKey>
    : SchemaAtKey<S, key> extends AnyZodSchema
      ? PrimitiveOrInaccessible<SchemaAtKey<S, key>>
      : "NEVER GET HERE";
};

export type CoMapKeys<
  S extends CoreCoMapSchema | CoreCoRecordSchema | CoreAccountSchema,
> = S extends CoreCoMapSchema
  ?
      | (keyof S["shape"] & string)
      | (S["catchAll"] extends AnyZodOrCoValueSchema ? string : never)
  : S extends CoreCoRecordSchema
    ? TypeOfZodSchema<S["keyType"]>
    : S extends CoreAccountSchema
      ? keyof S["shape"] & string
      : never;

export type SchemaAtKey<
  S extends CoreCoMapSchema | CoreCoRecordSchema | CoreAccountSchema,
  K extends CoMapKeys<S>,
> = S extends CoreCoMapSchema | CoreAccountSchema
  ? K extends keyof S["shape"]
    ? S["shape"][K]
    : S["catchAll"] extends AnyZodOrCoValueSchema
      ? S["catchAll"]
      : never
  : S extends CoreCoRecordSchema
    ? K extends S["keyType"]
      ? S["valueType"]
      : never
    : never;

// type CoMapLikeLoaded<
//   V extends object,
//   Depth,
//   DepthLimit extends number,
//   CurrentDepth extends number[],
// > =
//   IsUnion<LoadedAndRequired<V>> extends true
//     ? // Trigger conditional type distributivity to deeply resolve each member of the union separately
//       // Otherwise, deeply loaded values will resolve to `never`
//       V extends V
//       ? CoMapLikeLoaded<
//           V,
//           Pick<Depth, keyof V & keyof Depth>,
//           DepthLimit,
//           CurrentDepth
//         >
//       : never
//     : {
//         readonly [Key in keyof Omit<Depth, "$onError">]-?: Key extends CoKeys<V>
//           ? LoadedAndRequired<V[Key]> extends CoValue
//             ?
//                 | DeeplyLoaded<
//                     LoadedAndRequired<V[Key]>,
//                     Depth[Key],
//                     DepthLimit,
//                     [0, ...CurrentDepth]
//                   >
//                 | (undefined extends V[Key] ? undefined : never)
//                 | OnErrorResolvedValue<V[Key], Depth[Key]>
//             : never
//           : never;
//       } & V;

// export type DeeplyLoaded<
//   V,
//   Depth,
//   DepthLimit extends number = 10,
//   CurrentDepth extends number[] = [],
// > = DepthLimit extends CurrentDepth["length"]
//   ? V
//   : Depth extends true | undefined
//     ? V
//     : // Basically V extends CoList - but if we used that we'd introduce circularity into the definition of CoList itself
//       [V] extends [ReadonlyArray<infer Item>]
//       ? // `& {}` forces TypeScript to simplify the type before performing the `extends CoValue` check.
//         // Without it, the check would fail even when it should succeed.
//         AsLoaded<Item & {}> extends CoValue
//         ? Depth extends { $each: infer ItemDepth }
//           ? // Deeply loaded CoList
//             ReadonlyArray<
//               | DeeplyLoaded<
//                   AsLoaded<Item>,
//                   ItemDepth,
//                   DepthLimit,
//                   [0, ...CurrentDepth]
//                 >
//               | OnErrorResolvedValue<AsLoaded<Item>, Depth["$each"]>
//             > &
//               V // the CoList base type needs to be intersected after so that built-in methods return the correct narrowed array type
//           : never
//         : V
//       : // Basically V extends CoMap | Group | Account - but if we used that we'd introduce circularity into the definition of CoMap itself
//         [V] extends [{ [TypeSym]: "CoMap" | "Group" | "Account" }]
//         ? // If Depth = {} return V in any case
//           keyof Depth extends never
//           ? V
//           : // 1. Record-like CoMap
//             ItemsMarker extends keyof V
//             ? // 1.1. Deeply loaded Record-like CoMap with { $each: true | { $onError: 'catch' } }
//               Depth extends { $each: infer ItemDepth }
//               ? {
//                   readonly [key: string]:
//                     | DeeplyLoaded<
//                         LoadedAndRequired<V[ItemsMarker]>,
//                         ItemDepth,
//                         DepthLimit,
//                         [0, ...CurrentDepth]
//                       >
//                     | OnErrorResolvedValue<
//                         LoadedAndRequired<V[ItemsMarker]>,
//                         Depth["$each"]
//                       >;
//                 } & V // same reason as in CoList
//               : // 1.2. Deeply loaded Record-like CoMap with { [key: string]: true }
//                 string extends keyof Depth
//                 ? // if at least one key is `string`, then we treat the resolve as it was empty
//                   DeeplyLoaded<V, {}, DepthLimit, [0, ...CurrentDepth]> & V
//                 : // 1.3 Deeply loaded Record-like CoMap with single keys
//                   CoMapLikeLoaded<V, Depth, DepthLimit, CurrentDepth>
//             : // 2. Deeply loaded CoMap
//               CoMapLikeLoaded<V, Depth, DepthLimit, CurrentDepth>
//         : [V] extends [
//               {
//                 [TypeSym]: "CoStream";
//                 byMe: CoFeedEntry<infer Item> | undefined;
//               },
//             ]
//           ? // Deeply loaded CoStream
//             {
//               byMe?: { value: AsLoaded<Item> };
//               inCurrentSession?: { value: AsLoaded<Item> };
//               perSession: {
//                 [key: SessionID]: { value: AsLoaded<Item> };
//               };
//             } & { [key: ID<Account>]: { value: AsLoaded<Item> } } & V // same reason as in CoList
//           : [V] extends [
//                 {
//                   [TypeSym]: "BinaryCoStream";
//                 },
//               ]
//             ? V
//             : [V] extends [
//                   {
//                     [TypeSym]: "CoPlainText";
//                   },
//                 ]
//               ? V
//               : never;

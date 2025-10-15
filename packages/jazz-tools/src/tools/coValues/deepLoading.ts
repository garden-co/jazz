import { SessionID } from "cojson";
import { CoValueLoadingState, ItemsSym, TypeSym } from "../internal.js";
import { type Account } from "./account.js";
import { CoFeedEntry } from "./coFeed.js";
import { type CoKeys } from "./coMap.js";
import { type CoValue, type ID } from "./interfaces.js";

/**
 * Used to check if T is a union type.
 *
 * If T is a union type, the left hand side of the extends becomes a union of function types.
 * The right hand side is always a single function type.
 */
type IsUnion<T, U = T> = (T extends any ? (x: T) => void : never) extends (
  x: U,
) => void
  ? false
  : true;

/**
 * A CoValue that may or may not be loaded.
 */
// T should extend CoValue. We can't enforce this because it would introduce circularity
// into the definition of CoValues.
export type MaybeLoaded<T> = T | Unloaded<T>;

/**
 * A CoValue that is not loaded.
 */
export type Unloaded<T> = {
  $jazz: {
    id: ID<T>;
    loadingState:
      | typeof CoValueLoadingState.UNLOADED
      | typeof CoValueLoadingState.UNAVAILABLE
      | typeof CoValueLoadingState.UNAUTHORIZED;
  };
  $isLoaded: false;
};

/**
 * Narrows a maybe-loaded, optional CoValue to a loaded and required CoValue.
 */
export type LoadedAndRequired<T> = Exclude<T, undefined> & {
  $isLoaded: true;
};

/**
 * Narrows a maybe-loaded, optional CoValue to a loaded and optional CoValue
 */
export type AsLoaded<T> = LoadedAndRequired<T> extends CoValue
  ? T extends undefined
    ? LoadedAndRequired<T> | undefined
    : LoadedAndRequired<T>
  : T;

/**
 * By default, if a nested CoValue is not loaded, the parent CoValue will not be loaded either.
 * When `$onError: "catch"` is used, the parent CoValue will always be loaded, and an {@link Unloaded}
 * value will be returned for the nested CoValue if it cannot be loaded.
 *
 * Use `$onError` to handle cases where some data you have requested is inaccessible,
 * similar to a `try...catch` block in your query.
 */
type OnError = { $onError?: "catch" };

export type RefsToResolve<
  V,
  DepthLimit extends number = 10,
  CurrentDepth extends number[] = [],
> =
  | boolean
  | (DepthLimit extends CurrentDepth["length"]
      ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
        any
      : IsUnion<LoadedAndRequired<V>> extends true
        ? true
        : // Basically V extends CoList - but if we used that we'd introduce circularity into the definition of CoList itself
          V extends ReadonlyArray<infer Item>
          ?
              | ({
                  $each?: RefsToResolve<
                    AsLoaded<Item>,
                    DepthLimit,
                    [0, ...CurrentDepth]
                  >;
                } & OnError)
              | boolean
          : // Basically V extends CoMap | Group | Account - but if we used that we'd introduce circularity into the definition of CoMap itself
            V extends { [TypeSym]: "CoMap" | "Group" | "Account" }
            ?
                | ({
                    [Key in CoKeys<V> as LoadedAndRequired<
                      V[Key]
                    > extends CoValue
                      ? Key
                      : never]?: RefsToResolve<
                      LoadedAndRequired<V[Key]>,
                      DepthLimit,
                      [0, ...CurrentDepth]
                    >;
                  } & OnError)
                | (ItemsSym extends keyof V
                    ? {
                        $each: RefsToResolve<
                          LoadedAndRequired<V[ItemsSym]>,
                          DepthLimit,
                          [0, ...CurrentDepth]
                        >;
                      } & OnError
                    : never)
                | boolean
            : V extends {
                  [TypeSym]: "CoStream";
                  byMe: CoFeedEntry<infer Item> | undefined;
                }
              ?
                  | ({
                      $each: RefsToResolve<
                        AsLoaded<Item>,
                        DepthLimit,
                        [0, ...CurrentDepth]
                      >;
                    } & OnError)
                  | boolean
              : boolean);

export type RefsToResolveStrict<T, V> = [V] extends [RefsToResolve<T>]
  ? RefsToResolve<T>
  : V;

export type Resolved<
  T,
  R extends RefsToResolve<T> | undefined = true,
> = DeeplyLoaded<T, R, 10, []>;

/**
 * If the resolve query contains `$onError: "catch"`, we return an unloaded value for this nested CoValue.
 * Otherwise, the whole load operation returns an unloaded value.
 */
type OnErrorResolvedValue<V, Depth> = Depth extends { $onError: "catch" }
  ? Unloaded<V>
  : never;

type CoMapLikeLoaded<
  V extends object,
  Depth,
  DepthLimit extends number,
  CurrentDepth extends number[],
> = {
  readonly [Key in keyof Omit<Depth, "$onError">]-?: Key extends CoKeys<V>
    ? LoadedAndRequired<V[Key]> extends CoValue
      ?
          | DeeplyLoaded<
              LoadedAndRequired<V[Key]>,
              Depth[Key],
              DepthLimit,
              [0, ...CurrentDepth]
            >
          | (undefined extends V[Key] ? undefined : never)
          | OnErrorResolvedValue<V[Key], Depth[Key]>
      : never
    : never;
} & V;

export type DeeplyLoaded<
  V,
  Depth,
  DepthLimit extends number = 10,
  CurrentDepth extends number[] = [],
> = DepthLimit extends CurrentDepth["length"]
  ? V
  : Depth extends true | undefined
    ? V
    : // Basically V extends CoList - but if we used that we'd introduce circularity into the definition of CoList itself
      [V] extends [ReadonlyArray<infer Item>]
      ? // `& {}` forces TypeScript to simplify the type before performing the `extends CoValue` check.
        // Without it, the check would fail even when it should succeed.
        AsLoaded<Item & {}> extends CoValue
        ? Depth extends { $each: infer ItemDepth }
          ? // Deeply loaded CoList
            ReadonlyArray<
              | DeeplyLoaded<
                  AsLoaded<Item>,
                  ItemDepth,
                  DepthLimit,
                  [0, ...CurrentDepth]
                >
              | OnErrorResolvedValue<AsLoaded<Item>, Depth["$each"]>
            > &
              V // the CoList base type needs to be intersected after so that built-in methods return the correct narrowed array type
          : never
        : V
      : // Basically V extends CoMap | Group | Account - but if we used that we'd introduce circularity into the definition of CoMap itself
        [V] extends [{ [TypeSym]: "CoMap" | "Group" | "Account" }]
        ? // If Depth = {} return V in any case
          keyof Depth extends never
          ? V
          : // 1. Record-like CoMap
            ItemsSym extends keyof V
            ? // 1.1. Deeply loaded Record-like CoMap with { $each: true | { $onError: 'catch' } }
              Depth extends { $each: infer ItemDepth }
              ? {
                  readonly [key: string]:
                    | DeeplyLoaded<
                        LoadedAndRequired<V[ItemsSym]>,
                        ItemDepth,
                        DepthLimit,
                        [0, ...CurrentDepth]
                      >
                    | OnErrorResolvedValue<
                        LoadedAndRequired<V[ItemsSym]>,
                        Depth["$each"]
                      >;
                } & V // same reason as in CoList
              : // 1.2. Deeply loaded Record-like CoMap with { [key: string]: true }
                string extends keyof Depth
                ? // if at least one key is `string`, then we treat the resolve as it was empty
                  DeeplyLoaded<V, {}, DepthLimit, [0, ...CurrentDepth]> & V
                : // 1.3 Deeply loaded Record-like CoMap with single keys
                  CoMapLikeLoaded<V, Depth, DepthLimit, CurrentDepth>
            : // 2. Deeply loaded CoMap
              CoMapLikeLoaded<V, Depth, DepthLimit, CurrentDepth>
        : [V] extends [
              {
                [TypeSym]: "CoStream";
                byMe: CoFeedEntry<infer Item> | undefined;
              },
            ]
          ? // Deeply loaded CoStream
            {
              byMe?: { value: AsLoaded<Item> };
              inCurrentSession?: { value: AsLoaded<Item> };
              perSession: {
                [key: SessionID]: { value: AsLoaded<Item> };
              };
            } & { [key: ID<Account>]: { value: AsLoaded<Item> } } & V // same reason as in CoList
          : [V] extends [
                {
                  [TypeSym]: "BinaryCoStream";
                },
              ]
            ? V
            : [V] extends [
                  {
                    [TypeSym]: "CoPlainText";
                  },
                ]
              ? V
              : never;

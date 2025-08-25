import { SessionID } from "cojson";
import { ItemsSym, TypeSym } from "../internal.js";
import { type Account } from "./account.js";
import { CoFeedEntry } from "./coFeed.js";
import { type CoKeys } from "./coMap.js";
import { type CoValue, type ID } from "./interfaces.js";

/**
 * Similar to {@link NonNullable}, but removes only `null` and preserves `undefined`.
 */
export type NotNull<T> = Exclude<T, null>;

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

export type RefsToResolve<
  V,
  DepthLimit extends number = 10,
  CurrentDepth extends number[] = [],
> =
  | boolean
  | (DepthLimit extends CurrentDepth["length"]
      ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
        any
      : IsUnion<NonNullable<V>> extends true
        ? true
        : // Basically V extends CoList - but if we used that we'd introduce circularity into the definition of CoList itself
          V extends ReadonlyArray<infer Item>
          ?
              | {
                  $each?: RefsToResolve<
                    NotNull<Item>,
                    DepthLimit,
                    [0, ...CurrentDepth]
                  >;
                  $onError?: null;
                }
              | boolean
          : // Basically V extends CoMap | Group | Account - but if we used that we'd introduce circularity into the definition of CoMap itself
            V extends { [TypeSym]: "CoMap" | "Group" | "Account" }
            ?
                | ({
                    [Key in CoKeys<V> as NonNullable<V[Key]> extends CoValue
                      ? Key
                      : never]?: RefsToResolve<
                      NonNullable<V[Key]>,
                      DepthLimit,
                      [0, ...CurrentDepth]
                    >;
                  } & { $onError?: null })
                | (ItemsSym extends keyof V
                    ? {
                        $each: RefsToResolve<
                          NonNullable<V[ItemsSym]>,
                          DepthLimit,
                          [0, ...CurrentDepth]
                        >;
                        $onError?: null;
                      }
                    : never)
                | boolean
            : V extends {
                  [TypeSym]: "CoStream";
                  byMe: CoFeedEntry<infer Item> | undefined;
                }
              ?
                  | {
                      $each: RefsToResolve<
                        NotNull<Item>,
                        DepthLimit,
                        [0, ...CurrentDepth]
                      >;
                      $onError?: null;
                    }
                  | boolean
              : boolean);

export type RefsToResolveStrict<T, V> = V extends RefsToResolve<T>
  ? RefsToResolve<T>
  : V;

export type Resolved<
  T,
  R extends RefsToResolve<T> | undefined = true,
> = DeeplyLoaded<T, R, 10, []>;

type onErrorNullEnabled<Depth> = Depth extends { $onError: null }
  ? null
  : never;

type CoMapLikeLoaded<
  V extends object,
  Depth,
  DepthLimit extends number,
  CurrentDepth extends number[],
> = {
  readonly [Key in keyof Depth]-?: Key extends CoKeys<V>
    ? NonNullable<V[Key]> extends CoValue
      ?
          | DeeplyLoaded<
              NonNullable<V[Key]>,
              Depth[Key],
              DepthLimit,
              [0, ...CurrentDepth]
            >
          | (undefined extends V[Key] ? undefined : never)
          | onErrorNullEnabled<Depth[Key]>
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
  : Depth extends boolean | undefined // Checking against boolean instead of true because the inference from RefsToResolveStrict transforms true into boolean
    ? V
    : // Basically V extends CoList - but if we used that we'd introduce circularity into the definition of CoList itself
      [V] extends [ReadonlyArray<infer Item>]
      ? NotNull<Item> extends CoValue
        ? Depth extends { $each: infer ItemDepth }
          ? // Deeply loaded CoList
            ReadonlyArray<
              | (NotNull<Item> &
                  DeeplyLoaded<
                    NotNull<Item>,
                    ItemDepth,
                    DepthLimit,
                    [0, ...CurrentDepth]
                  >)
              | onErrorNullEnabled<Depth["$each"]>
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
            ? // 1.1. Deeply loaded Record-like CoMap with { $each: true | {$onError: null} }
              Depth extends { $each: infer ItemDepth }
              ? {
                  readonly [key: string]:
                    | DeeplyLoaded<
                        NonNullable<V[ItemsSym]>,
                        ItemDepth,
                        DepthLimit,
                        [0, ...CurrentDepth]
                      >
                    | onErrorNullEnabled<Depth["$each"]>;
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
              byMe?: { value: NotNull<Item> };
              inCurrentSession?: { value: NotNull<Item> };
              perSession: {
                [key: SessionID]: { value: NotNull<Item> };
              };
            } & { [key: ID<Account>]: { value: NotNull<Item> } } & V // same reason as in CoList
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

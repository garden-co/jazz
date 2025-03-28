import { SessionID } from "cojson";
import { ItemsSym, type Ref, RefEncoded, UnCo } from "../internal.js";
import { type Account } from "./account.js";
import { type CoFeed, CoFeedEntry } from "./coFeed.js";
import { type CoList } from "./coList.js";
import { type CoKeys, type CoMap } from "./coMap.js";
import { type CoValue, type ID } from "./interfaces.js";

function hasRefValue(value: CoValue, key: string | number) {
  return Boolean(
    (
      value as unknown as {
        _refs: { [key: string]: Ref<CoValue> | undefined };
      }
    )._refs?.[key],
  );
}

function hasReadAccess(value: CoValue, key: string | number) {
  return Boolean(
    (
      value as unknown as {
        _refs: { [key: string]: Ref<CoValue> | undefined };
      }
    )._refs?.[key]?.hasReadAccess(),
  );
}

function isOptionalField(value: CoValue, key: string): boolean {
  return (
    ((value as CoMap)._schema[key] as RefEncoded<CoValue>)?.optional ?? false
  );
}

type FulfillsDepthResult = "unauthorized" | "fulfilled" | "unfulfilled";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function fulfillsDepth(depth: any, value: CoValue): FulfillsDepthResult {
  if (depth === true || depth === undefined) {
    return "fulfilled";
  }

  if (
    value._type === "CoMap" ||
    value._type === "Group" ||
    value._type === "Account"
  ) {
    const map = value as CoMap;

    if ("$each" in depth) {
      let result: FulfillsDepthResult = "fulfilled";

      for (const [key, item] of Object.entries(value)) {
        if (map._raw.get(key) !== undefined) {
          if (!item) {
            if (hasReadAccess(map, key)) {
              result = "unfulfilled";
              continue;
            } else {
              return "unauthorized";
            }
          }

          const innerResult = fulfillsDepth(depth.$each, item);

          if (innerResult === "unfulfilled") {
            result = "unfulfilled";
          } else if (
            innerResult === "unauthorized" &&
            !isOptionalField(value, ItemsSym)
          ) {
            return "unauthorized"; // If any item is unauthorized, the whole thing is unauthorized
          }
        } else if (!isOptionalField(value, ItemsSym)) {
          return "unfulfilled";
        }
      }

      return result;
    } else {
      let result: FulfillsDepthResult = "fulfilled";

      for (const key of Object.keys(depth)) {
        if (map._raw.get(key) === undefined) {
          if (!map._schema?.[key]) {
            // Field not defined in schema
            if (map._schema?.[ItemsSym]) {
              // CoMap.Record
              if (isOptionalField(map, ItemsSym)) {
                continue;
              } else {
                // All fields are required, so the returned type is not optional and we must comply
                throw new Error(
                  `The ref ${key} requested on ${map.constructor.name} is missing`,
                );
              }
            } else {
              // Field not defined in CoMap schema
              throw new Error(
                `The ref ${key} requested on ${map.constructor.name} is not defined in the schema`,
              );
            }
          } else if (isOptionalField(map, key)) {
            continue;
          } else {
            // Field is required but has never been set
            throw new Error(
              `The ref ${key} on ${map.constructor.name} is required but missing`,
            );
          }
        } else {
          const item = (value as Record<string, any>)[key];

          if (!item) {
            if (hasReadAccess(map, key)) {
              result = "unfulfilled";
              continue;
            } else {
              return "unauthorized";
            }
          }

          const innerResult = fulfillsDepth(depth[key], item);

          if (innerResult === "unfulfilled") {
            result = "unfulfilled";
          } else if (
            innerResult === "unauthorized" &&
            !isOptionalField(value, key)
          ) {
            return "unauthorized"; // If any item is unauthorized, the whole thing is unauthorized
          }
        }
      }

      return result;
    }
  } else if (value._type === "CoList") {
    if ("$each" in depth) {
      let result: FulfillsDepthResult = "fulfilled";

      for (const [key, item] of (value as CoList).entries()) {
        if (hasRefValue(value, key)) {
          if (!item) {
            if (hasReadAccess(value, key)) {
              result = "unfulfilled";
              continue;
            } else {
              return "unauthorized";
            }
          }

          const innerResult = fulfillsDepth(depth.$each, item);

          if (innerResult === "unfulfilled") {
            result = "unfulfilled";
          } else if (
            innerResult === "unauthorized" &&
            !isOptionalField(value, ItemsSym)
          ) {
            return "unauthorized"; // If any item is unauthorized, the whole thing is unauthorized
          }
        } else if (!isOptionalField(value, ItemsSym)) {
          return "unfulfilled";
        }
      }

      return result;
    }

    return "fulfilled";
  } else if (value._type === "CoStream") {
    if ("$each" in depth) {
      let result: FulfillsDepthResult = "fulfilled";

      for (const item of Object.values((value as CoFeed).perSession)) {
        if (item.ref) {
          if (!item.value) {
            if (item.ref.hasReadAccess()) {
              result = "unfulfilled";
              continue;
            } else {
              return "unauthorized";
            }
          }

          const innerResult = fulfillsDepth(depth.$each, item.value);

          if (innerResult === "unfulfilled") {
            result = "unfulfilled";
          } else if (
            innerResult === "unauthorized" &&
            !isOptionalField(value, ItemsSym)
          ) {
            return "unauthorized"; // If any item is unauthorized, the whole thing is unauthorized
          }
        } else if (!isOptionalField(value, ItemsSym)) {
          return "unfulfilled";
        }
      }

      return result;
    }

    return "fulfilled";
  } else if (
    value._type === "BinaryCoStream" ||
    value._type === "CoPlainText"
  ) {
    return "fulfilled";
  } else {
    console.error(value);
    throw new Error("Unexpected value type: " + value._type);
  }
}

type UnCoNotNull<T> = UnCo<Exclude<T, null>>;
export type Clean<T> = UnCo<NonNullable<T>>;

export type RefsToResolve<
  V,
  DepthLimit extends number = 10,
  CurrentDepth extends number[] = [],
> =
  | boolean
  | (DepthLimit extends CurrentDepth["length"]
      ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
        any
      : // Basically V extends CoList - but if we used that we'd introduce circularity into the definition of CoList itself
        V extends Array<infer Item>
        ?
            | {
                $each: RefsToResolve<
                  UnCoNotNull<Item>,
                  DepthLimit,
                  [0, ...CurrentDepth]
                >;
              }
            | boolean
        : // Basically V extends CoMap | Group | Account - but if we used that we'd introduce circularity into the definition of CoMap itself
          V extends { _type: "CoMap" | "Group" | "Account" }
          ?
              | {
                  [Key in CoKeys<V> as Clean<V[Key]> extends CoValue
                    ? Key
                    : never]?: RefsToResolve<
                    Clean<V[Key]>,
                    DepthLimit,
                    [0, ...CurrentDepth]
                  >;
                }
              | (ItemsSym extends keyof V
                  ? {
                      $each: RefsToResolve<
                        Clean<V[ItemsSym]>,
                        DepthLimit,
                        [0, ...CurrentDepth]
                      >;
                    }
                  : never)
              | boolean
          : V extends {
                _type: "CoStream";
                byMe: CoFeedEntry<infer Item> | undefined;
              }
            ?
                | {
                    $each: RefsToResolve<
                      UnCoNotNull<Item>,
                      DepthLimit,
                      [0, ...CurrentDepth]
                    >;
                  }
                | boolean
            : boolean);

export type RefsToResolveStrict<T, V> = V extends RefsToResolve<T>
  ? RefsToResolve<T>
  : V;

export type Resolved<T, R extends RefsToResolve<T> | undefined> = DeeplyLoaded<
  T,
  R,
  10,
  []
>;

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
      [V] extends [Array<infer Item>]
      ? UnCoNotNull<Item> extends CoValue
        ? Depth extends { $each: infer ItemDepth }
          ? // Deeply loaded CoList
            (UnCoNotNull<Item> &
              DeeplyLoaded<
                UnCoNotNull<Item>,
                ItemDepth,
                DepthLimit,
                [0, ...CurrentDepth]
              >)[] &
              V // the CoList base type needs to be intersected after so that built-in methods return the correct narrowed array type
          : never
        : V
      : // Basically V extends CoMap | Group | Account - but if we used that we'd introduce circularity into the definition of CoMap itself
        [V] extends [{ _type: "CoMap" | "Group" | "Account" }]
        ? ItemsSym extends keyof V
          ? Depth extends { $each: infer ItemDepth }
            ? // Deeply loaded Record-like CoMap
              {
                [key: string]: DeeplyLoaded<
                  Clean<V[ItemsSym]>,
                  ItemDepth,
                  DepthLimit,
                  [0, ...CurrentDepth]
                >;
              } & V // same reason as in CoList
            : never
          : keyof Depth extends never // Depth = {}
            ? V
            : // Deeply loaded CoMap
              {
                -readonly [Key in keyof Depth]-?: Key extends CoKeys<V>
                  ? Clean<V[Key]> extends CoValue
                    ?
                        | DeeplyLoaded<
                            Clean<V[Key]>,
                            Depth[Key],
                            DepthLimit,
                            [0, ...CurrentDepth]
                          >
                        | (undefined extends V[Key] ? undefined : never)
                    : never
                  : never;
              } & V // same reason as in CoList
        : [V] extends [
              {
                _type: "CoStream";
                byMe: CoFeedEntry<infer Item> | undefined;
              },
            ]
          ? // Deeply loaded CoStream
            {
              byMe?: { value: UnCoNotNull<Item> };
              inCurrentSession?: { value: UnCoNotNull<Item> };
              perSession: {
                [key: SessionID]: { value: UnCoNotNull<Item> };
              };
            } & { [key: ID<Account>]: { value: UnCoNotNull<Item> } } & V // same reason as in CoList
          : [V] extends [
                {
                  _type: "BinaryCoStream";
                },
              ]
            ? V
            : [V] extends [
                  {
                    _type: "CoPlainText";
                  },
                ]
              ? V
              : never;

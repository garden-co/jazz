import { CoFeed } from "./coFeed.js";
import { CoList } from "./coList.js";
import { CoMap, CoMapInit } from "./coMap.js";
import { CoPlainText } from "./coPlainText.js";
import { CoRichText } from "./coRichText.js";
import { CoVector } from "./coVector.js";

/**
 * Returns the type of values that can be used to initialize a field of the provided type.
 *
 * For CoValue references, either a CoValue of the same type, or a plain JSON value that can be
 * converted to the CoValue type are allowed.
 */
// Note: we don't define this type as V | ... because it prevents TS from inlining the type
export type CoFieldInit<V> = V extends CoMap
  ? V | CoMapInit<V>
  : V extends CoList<infer T> | CoFeed<infer T>
    ? V | ReadonlyArray<CoFieldInit<T>>
    : V extends CoVector | Readonly<CoVector>
      ? V | ReadonlyArray<number> | Float32Array
      : V extends CoPlainText | CoRichText
        ? V | string
        : V;

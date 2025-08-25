import { CoFeed } from "./coFeed";
import { CoList } from "./coList";
import { CoMap, CoMapInit } from "./coMap";
import { CoPlainText } from "./coPlainText";
import { CoRichText } from "./coRichText";

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
    : V extends CoPlainText | CoRichText
      ? V | string
      : V;

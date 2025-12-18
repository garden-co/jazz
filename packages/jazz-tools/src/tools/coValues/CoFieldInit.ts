import { TypeOfZodSchema } from "../implementation/zodSchema/typeConverters/TypeOfZodSchema.js";
import {
  CoreCoFeedSchema,
  CoreCoListSchema,
  CoreCoMapSchema,
  CoreCoVectorSchema,
  CoreRichTextSchema,
  CorePlainTextSchema,
  Loaded,
  AnyZodSchema,
} from "../internal.js";
import { CoMapInit } from "./coMap.js";

/**
 * Returns the type of values that can be used to initialize a field of the provided type.
 *
 * For CoValue references, either a CoValue of the same type, or a plain JSON value that can be
 * converted to the CoValue type are allowed.
 */
export type CoFieldInit<S> = S extends CoreCoMapSchema
  ? Loaded<S> | CoMapInit<S>
  : S extends CoreCoListSchema<infer T> | CoreCoFeedSchema<infer T>
    ? Loaded<S> | ReadonlyArray<CoFieldInit<T>>
    : S extends CoreCoVectorSchema
      ? Loaded<S> | ReadonlyArray<number> | Float32Array
      : S extends CorePlainTextSchema | CoreRichTextSchema
        ? Loaded<S> | string
        : S extends AnyZodSchema
          ? TypeOfZodSchema<S>
          : never;

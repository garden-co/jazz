import type { JsonValue } from "cojson";
import { coField } from "./schema";

type JsonValueWithoutNull = Exclude<JsonValue, null>;

export type InstanceEncoder<V> = {
  encode: (value: V) => JsonValueWithoutNull;
  decode: (value: JsonValueWithoutNull) => V;
};

export const registeredInstanceEncoders = new Map<
  new (
    ...args: any
  ) => any,
  any
>();

/**
 * Registers an encoder for a class. The encoder is used to encode and decode instances of the class when they are stored within a CoValue.
 *
 * This encoder will be later used when class instances are stored or loaded within a CoValue when referred to using `z.instanceof(...)`
 *
 * @param cls The class to register the encoder for.
 * @param encoder The encoder to register.
 *
 * @example
 * ```typescript
 * class DateRange {
 *   constructor(
 *     public start: Date,
 *     public end: Date,
 *   ) {}
 * }
 *
 * encoders.register(DateRange, {
 *   encode: (value) => [value.start.toISOString(), value.end.toISOString()],
 *   decode: (value) => {
 *     const [start, end] = value as [string, string];
 *     return new DateRange(new Date(start), new Date(end));
 *   },
 * });
 */
export const registerInstanceEncoder = <T>(
  cls: new (...args: any) => T,
  encoder: InstanceEncoder<T>,
) => {
  registeredInstanceEncoders.set(
    cls,
    coField.optional.encoded({
      encode: (value: T | null | undefined) => {
        if (value === undefined) return undefined as unknown as JsonValue;
        if (value === null) return null;
        return encoder.encode(value);
      },
      decode: (value) => {
        if (value === null) return null;
        if (value === undefined) return undefined;
        return encoder.decode(value);
      },
    }),
  );
};

/**
 * Unregisters an encoder for a class.
 *
 * @param cls The class to unregister the encoder for.
 */
export const unregisterInstanceEncoder = (cls: new (...args: any) => any) => {
  registeredInstanceEncoders.delete(cls);
};

/**
 * Unregisters all encoders.
 */
export const unregisterAllInstanceEncoders = () => {
  registeredInstanceEncoders.clear();
};

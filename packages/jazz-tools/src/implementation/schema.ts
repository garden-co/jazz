import type { JsonValue, RawCoValue } from "cojson";
import { CojsonInternalTypes } from "cojson";
import {
  type CoValue,
  type CoValueClass,
  CoValueFromRaw,
  ItemsSym,
  JazzToolsSymbol,
  SchemaInit,
  isCoValueClass,
} from "../internal.js";

/** @category Schema definition */
export const Encoders = {
  Date: {
    encode: (value: Date) => value.toISOString(),
    decode: (value: JsonValue) => new Date(value as string),
  },
  OptionalDate: {
    encode: (value: Date | undefined) => value?.toISOString() || null,
    decode: (value: JsonValue) =>
      value === null ? undefined : new Date(value as string),
  },
};

const optional = {
  ref: optionalRef,
  json<T extends CojsonInternalTypes.CoJsonValue<T>>(): T | undefined {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { [SchemaInit]: "json" satisfies Schema } as any;
  },
  encoded<T>(arg: OptionalEncoder<T>): T | undefined {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { [SchemaInit]: { encoded: arg } satisfies Schema } as any;
  },
  string: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as string | undefined,
  number: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as number | undefined,
  boolean: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as boolean | undefined,
  null: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as null | undefined,
  Date: {
    [SchemaInit]: { encoded: Encoders.OptionalDate } satisfies Schema,
  } as unknown as Date | undefined,
  literal<T extends (string | number | boolean)[]>(
    ..._lit: T
  ): T[number] | undefined {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { [SchemaInit]: "json" satisfies Schema } as any;
  },
};

/** @category Schema definition */
export const coField = {
  string: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as string,
  number: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as number,
  boolean: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as boolean,
  null: {
    [SchemaInit]: "json" satisfies Schema,
  } as unknown as null,
  Date: {
    [SchemaInit]: { encoded: Encoders.Date } satisfies Schema,
  } as unknown as Date,
  literal<T extends (string | number | boolean)[]>(..._lit: T): T[number] {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { [SchemaInit]: "json" satisfies Schema } as any;
  },
  json<T extends CojsonInternalTypes.CoJsonValue<T>>(): T {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { [SchemaInit]: "json" satisfies Schema } as any;
  },
  encoded<T>(arg: Encoder<T>): T {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return { [SchemaInit]: { encoded: arg } satisfies Schema } as any;
  },
  ref,
  items: ItemsSym as ItemsSym,
  optional,
};

function optionalRef<C extends CoValueClass>(
  arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
): InstanceType<C> | null | undefined {
  return ref(arg, { optional: true });
}

function ref<C extends CoValueClass>(
  arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
  options?: never,
): InstanceType<C> | null;
function ref<C extends CoValueClass>(
  arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
  options: { optional: true },
): InstanceType<C> | null | undefined;
function ref<
  C extends CoValueClass,
  Options extends { optional?: boolean } | undefined,
>(
  arg: C | ((_raw: InstanceType<C>["_raw"]) => C),
  options?: Options,
): Options extends { optional: true }
  ? InstanceType<C> | null | undefined
  : InstanceType<C> | null {
  return {
    [SchemaInit]: {
      ref: arg,
      optional: options?.optional || false,
    } satisfies Schema,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
}

export type JsonEncoded = "json";
export type EncodedAs<V> = { encoded: Encoder<V> | OptionalEncoder<V> };
export type RefEncoded<V extends CoValue> = {
  ref: CoValueClass<V> | ((raw: RawCoValue) => CoValueClass<V>);
  optional: boolean;
};

export function isRefEncoded<V extends CoValue>(
  schema: Schema,
): schema is RefEncoded<V> {
  return (
    typeof schema === "object" &&
    "ref" in schema &&
    "optional" in schema &&
    typeof schema.ref === "function"
  );
}

export function instantiateRefEncoded<V extends CoValue>(
  schema: RefEncoded<V>,
  raw: RawCoValue,
): V {
  return isCoValueClass<V>(schema.ref)
    ? schema.ref.fromRaw(raw)
    : (schema.ref as (raw: RawCoValue) => CoValueClass<V> & CoValueFromRaw<V>)(
        raw,
      ).fromRaw(raw);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Schema = JsonEncoded | RefEncoded<CoValue> | EncodedAs<any>;

export type SchemaFor<Field> = NonNullable<Field> extends CoValue
  ? RefEncoded<NonNullable<Field>>
  : NonNullable<Field> extends JsonValue
    ? JsonEncoded
    : EncodedAs<NonNullable<Field>>;

export type Encoder<V> = {
  encode: (value: V) => JsonValue;
  decode: (value: JsonValue) => V;
};
export type OptionalEncoder<V> =
  | Encoder<V>
  | {
      encode: (value: V | undefined) => JsonValue;
      decode: (value: JsonValue) => V | undefined;
    };

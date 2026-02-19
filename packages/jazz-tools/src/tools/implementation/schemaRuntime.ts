import type { CoValueUniqueness, JsonValue, RawCoValue } from "cojson";
import {
  CoValue,
  type CoValueClass,
  CoValueFromRaw,
  type GlobalValidationMode,
  Group,
  LoadedAndRequired,
  type NewInlineOwnerStrategy,
  type RefOnCreateCallback,
  type RefPermissions,
  extendContainerOwner,
  isCoValueClass,
} from "../internal.js";

export type JsonEncoded = "json";
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
export type EncodedAs<V> = { encoded: Encoder<V> | OptionalEncoder<V> };
export type RefEncoded<V extends CoValue> = {
  ref: CoValueClass<V> | ((raw: RawCoValue) => CoValueClass<V>);
  optional: boolean;
  permissions?: RefPermissions;
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

export function instantiateRefEncodedFromRaw<V extends CoValue>(
  schema: RefEncoded<V>,
  raw: RawCoValue,
): V {
  return isCoValueClass<V>(schema.ref)
    ? schema.ref.fromRaw(raw)
    : (schema.ref as (raw: RawCoValue) => CoValueClass<V> & CoValueFromRaw<V>)(
        raw,
      ).fromRaw(raw);
}

/**
 * Derive a child uniqueness from a parent uniqueness and a field name.
 */
export function deriveChildUniqueness(
  parentUniqueness: CoValueUniqueness["uniqueness"],
  fieldName: string,
): CoValueUniqueness["uniqueness"] {
  if (typeof parentUniqueness === "string") {
    return `${parentUniqueness}@@${fieldName}`;
  }
  if (typeof parentUniqueness === "object" && parentUniqueness !== null) {
    const existingField = parentUniqueness._field ?? "";
    return {
      ...parentUniqueness,
      _field: existingField ? `${existingField}/${fieldName}` : fieldName,
    };
  }
  return parentUniqueness;
}

/**
 * Creates a new CoValue of the given ref type, using the provided init values.
 */
export function instantiateRefEncodedWithInit<V extends CoValue>(
  schema: RefEncoded<V>,
  init: any,
  containerOwner: Group,
  newOwnerStrategy: NewInlineOwnerStrategy = extendContainerOwner,
  onCreate?: RefOnCreateCallback,
  unique?: {
    uniqueness: CoValueUniqueness["uniqueness"];
    fieldName: string;
    firstComesWins: boolean;
  },
  validationMode?: GlobalValidationMode,
): V {
  const resolvedRef = isCoValueClass<V>(schema.ref)
    ? schema.ref
    : (schema.ref as (raw: RawCoValue) => CoValueClass<V>)(init as RawCoValue);

  if (!isCoValueClass<V>(resolvedRef)) {
    throw Error(
      `Cannot automatically create CoValue from value: ${JSON.stringify(init)}. Use the CoValue schema's create() method instead.`,
    );
  }
  const owner = newOwnerStrategy(() => Group.create(), containerOwner, init);
  onCreate?.(owner, init);

  let childUniqueness: CoValueUniqueness["uniqueness"] | undefined;
  if (unique !== undefined) {
    const isSameOwner = owner === containerOwner;
    if (isSameOwner) {
      childUniqueness = deriveChildUniqueness(
        unique.uniqueness,
        unique.fieldName,
      );
    } else if (
      typeof unique.uniqueness === "string" ||
      (typeof unique.uniqueness === "object" && unique.uniqueness !== null)
    ) {
      console.warn(
        `Inline CoValue at field "${unique.fieldName}" has a different owner than its unique parent. ` +
          `The child will not inherit uniqueness. Consider using "sameAsContainer" permission ` +
          `for CoValues within unique parents.`,
      );
    }
  }

  // @ts-expect-error - create is a static method in all CoValue classes
  return resolvedRef.create(init, {
    owner,
    validation: validationMode,
    unique: childUniqueness,
    firstComesWins: unique?.firstComesWins,
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Schema = JsonEncoded | RefEncoded<CoValue> | EncodedAs<any>;

export type SchemaFor<Field> = LoadedAndRequired<Field> extends CoValue
  ? RefEncoded<LoadedAndRequired<Field>>
  : LoadedAndRequired<Field> extends JsonValue
    ? JsonEncoded
    : EncodedAs<LoadedAndRequired<Field>>;

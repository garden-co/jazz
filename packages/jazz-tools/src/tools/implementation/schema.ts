import type { JsonValue, RawCoValue } from "cojson";
import { CojsonInternalTypes } from "cojson";
import {
  Account,
  type CoValue,
  type CoValueClass,
  CoValueFromRaw,
  extendContainerOwner,
  Group,
  type GroupRole,
  ItemsMarker,
  LoadedAndRequired,
  type NewInlineOwnerStrategy,
  type RefOnCreateCallback,
  type RefPermissions,
  isCoValueClass,
  co,
  SchemaField,
  ZodSchemaField,
} from "../internal.js";
import { CoreCoValueSchema } from "./zodSchema/schemaTypes/CoValueSchema.js";

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

export type JsonEncoded = { type: "json"; field: ZodSchemaField };
export type EncodedAs<V> = { type: "encoded"; field: ZodSchemaField } & (
  | Encoder<V>
  | OptionalEncoder<V>
);
export type RefEncoded<V extends CoValue> = {
  type: "ref";
  ref: CoValueClass<V> | ((raw: RawCoValue) => CoValueClass<V>);
  optional: boolean;
  permissions?: RefPermissions;
  sourceSchema: CoreCoValueSchema | CoValueClass;
};

export function isRefEncoded<V extends CoValue>(
  schema: FieldDescriptor,
): schema is RefEncoded<V> {
  return (
    typeof schema === "object" &&
    "type" in schema &&
    schema.type === "ref" &&
    "ref" in schema &&
    "optional" in schema &&
    typeof schema.ref === "function"
  );
}

export function instantiateRefEncodedFromRaw<V extends CoValue>(
  schema: RefEncoded<V>,
  raw: RawCoValue,
): V {
  if (!schema.sourceSchema) {
    throw new Error("sourceSchema is required");
  }

  if (
    "fromRaw" in schema.sourceSchema &&
    typeof schema.sourceSchema.fromRaw === "function"
  ) {
    // new path if CoValueSchema has a fromRaw method
    return schema.sourceSchema.fromRaw(raw);
  }
  if ((schema.ref as any).name && (schema.ref as any).name.includes("CoList")) {
    throw new Error("use the CoListSchema instead of the CoList class");
  }
  if (
    (schema.ref as any).name &&
    (schema.ref as any).name.includes("CoVector")
  ) {
    throw new Error("use the CoVectorSchema instead of the CoVector class");
  }
  if (
    (schema.ref as any).name &&
    (schema.ref as any).name.includes("CoPlainText")
  ) {
    throw new Error("use the PlainTextSchema instead of the CoPlainText class");
  }
  if (
    (schema.ref as any).name &&
    (schema.ref as any).name.includes("CoRichText")
  ) {
    throw new Error("use the RichTextSchema instead of the CoRichText class");
  }
  if (
    (schema.ref as any).name &&
    (schema.ref as any).name.includes("FileStream")
  ) {
    throw new Error("use the FileStreamSchema instead of the FileStream class");
  }
  return isCoValueClass<V>(schema.ref)
    ? schema.ref.fromRaw(raw)
    : (schema.ref as (raw: RawCoValue) => CoValueClass<V> & CoValueFromRaw<V>)(
        raw,
      ).fromRaw(raw);
}

/**
 * Creates a new CoValue of the given ref type, using the provided init values.
 *
 * @param schema - The schema of the CoValue to create.
 * @param init - The init values to use to create the CoValue.
 * @param containerOwner - The owner of the referencing CoValue. Will be used
 * to determine the owner of the new CoValue
 * @param newOwnerStrategy - The strategy to use to determine the owner of the new CoValue
 * @param onCreate - The callback to call when the new CoValue is created
 * @returns The created CoValue.
 */
export function instantiateRefEncodedWithInit<V extends CoValue>(
  schema: RefEncoded<V>,
  init: any,
  containerOwner: Group,
  newOwnerStrategy: NewInlineOwnerStrategy = extendContainerOwner,
  onCreate?: RefOnCreateCallback,
): V {
  if (
    !isCoValueClass<V>(schema.ref) &&
    !(
      "create" in schema.sourceSchema &&
      typeof schema.sourceSchema.create === "function"
    )
  ) {
    throw Error(
      `Cannot automatically create CoValue from value: ${JSON.stringify(init)}. Use the CoValue schema's create() method instead.`,
    );
  }
  const owner = newOwnerStrategy(
    () => co.group().create(),
    containerOwner,
    init,
  );
  onCreate?.(owner, init);

  if (
    "create" in schema.sourceSchema &&
    typeof schema.sourceSchema.create === "function"
  ) {
    // new path if CoValueSchema has a create method
    return schema.sourceSchema.create(init, owner);
  }
  // @ts-expect-error - create is a static method in all CoValue classes
  return schema.ref.create(init, owner);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FieldDescriptor =
  | JsonEncoded
  | RefEncoded<CoValue>
  | EncodedAs<any>;

export type FieldDescriptorFor<Field> = LoadedAndRequired<Field> extends CoValue
  ? RefEncoded<LoadedAndRequired<Field>>
  : LoadedAndRequired<Field> extends JsonValue
    ? JsonEncoded
    : EncodedAs<LoadedAndRequired<Field>>;

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

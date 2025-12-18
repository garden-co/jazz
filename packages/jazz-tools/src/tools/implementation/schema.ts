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
  Loaded,
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
export type RefEncoded<S extends CoreCoValueSchema = CoreCoValueSchema> = {
  type: "ref";
  optional: boolean;
  permissions?: RefPermissions;
  sourceSchema: CoreCoValueSchema;
};

export function isRefEncoded<S extends CoreCoValueSchema>(
  schema: FieldDescriptor,
): schema is RefEncoded<S> {
  return (
    typeof schema === "object" &&
    "type" in schema &&
    schema.type === "ref" &&
    "ref" in schema &&
    "optional" in schema &&
    typeof schema.ref === "function"
  );
}

export function instantiateRefEncodedFromRaw<S extends CoreCoValueSchema>(
  schema: RefEncoded<S>,
  raw: RawCoValue,
): Loaded<S> {
  if (!schema.sourceSchema) {
    throw new Error("sourceSchema is required");
  }

  if (
    "fromRaw" in schema.sourceSchema &&
    typeof schema.sourceSchema.fromRaw === "function"
  ) {
    // new path if CoValueSchema has a fromRaw method
    return schema.sourceSchema.fromRaw(raw);
  } else {
    throw new Error("fromRaw method not found on sourceSchema");
  }
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
export function instantiateRefEncodedWithInit<S extends CoreCoValueSchema>(
  schema: RefEncoded<S>,
  init: any,
  containerOwner: Group,
  newOwnerStrategy: NewInlineOwnerStrategy = extendContainerOwner,
  onCreate?: RefOnCreateCallback,
): Loaded<S> {
  if (
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
  | RefEncoded<CoreCoValueSchema>
  | EncodedAs<any>;

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

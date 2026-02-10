import type { CoValueUniqueness, JsonValue, RawCoValue } from "cojson";
import { CojsonInternalTypes } from "cojson";
import {
  Account,
  type CoValue,
  type CoValueClass,
  CoValueFromRaw,
  extendContainerOwner,
  Group,
  type GroupRole,
  ItemsSym,
  LoadedAndRequired,
  type NewInlineOwnerStrategy,
  type RefOnCreateCallback,
  type RefPermissions,
  SchemaInit,
  isCoValueClass,
  GlobalValidationMode,
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
  arg: C | ((raw: InstanceType<C>["$jazz"]["raw"]) => C),
  options: { permissions: RefPermissions },
): InstanceType<C> | null | undefined {
  return ref(arg, { optional: true, permissions: options.permissions });
}

function ref<C extends CoValueClass>(
  arg: C | ((raw: InstanceType<C>["$jazz"]["raw"]) => C),
  options: { permissions?: RefPermissions },
): InstanceType<C> | null;
function ref<C extends CoValueClass>(
  arg: C | ((raw: InstanceType<C>["$jazz"]["raw"]) => C),
  options: { optional: true; permissions?: RefPermissions },
): InstanceType<C> | null | undefined;
function ref<
  C extends CoValueClass,
  Options extends { optional?: boolean; permissions?: RefPermissions },
>(
  arg: C | ((raw: InstanceType<C>["$jazz"]["raw"]) => C),
  options: Options,
): Options extends { optional: true }
  ? InstanceType<C> | null | undefined
  : InstanceType<C> | null {
  return {
    [SchemaInit]: {
      ref: arg,
      optional: options.optional || false,
      permissions: options.permissions,
    } satisfies Schema,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
}

export type JsonEncoded = "json";
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
 *
 * For string uniqueness: `parentUnique + "/" + fieldName`
 * For object uniqueness: `{ ...parentUnique, _field: existingField + "/" + fieldName }`
 *
 * @param parentUniqueness - The parent's uniqueness value
 * @param fieldName - The name of the field containing the child
 * @returns The derived uniqueness for the child
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
  // For boolean/null/undefined, return as-is (no derivation needed)
  return parentUniqueness;
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
 * @param parentUniqueness - The parent's uniqueness value (if the parent is unique)
 * @param fieldName - The name of the field containing this ref (for deriving child uniqueness)
 * @returns The created CoValue.
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
  if (!isCoValueClass<V>(schema.ref)) {
    throw Error(
      `Cannot automatically create CoValue from value: ${JSON.stringify(init)}. Use the CoValue schema's create() method instead.`,
    );
  }
  const owner = newOwnerStrategy(() => Group.create(), containerOwner, init);
  onCreate?.(owner, init);

  // Derive child uniqueness if parent is unique and child uses the same owner
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
      // Log warning when parent has meaningful uniqueness but child uses a different owner
      console.warn(
        `Inline CoValue at field "${unique.fieldName}" has a different owner than its unique parent. ` +
          `The child will not inherit uniqueness. Consider using "sameAsContainer" permission ` +
          `for CoValues within unique parents.`,
      );
    }
  }

  // @ts-expect-error - create is a static method in all CoValue classes
  return schema.ref.create(init, {
    owner,
    validation: validationMode,
    unique: childUniqueness,
    firstComesWins: unique?.firstComesWins,
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Schema = JsonEncoded | RefEncoded<CoValue> | EncodedAs<any>;

export function isSchemaDescriptorValue(value: unknown): value is Schema {
  if (value === "json") {
    return true;
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }

  return (
    ("encoded" in value &&
      typeof (value as { encoded?: unknown }).encoded === "object") ||
    ("ref" in value &&
      "optional" in value &&
      typeof (value as { ref?: unknown }).ref === "function")
  );
}

export type SchemaFor<Field> = LoadedAndRequired<Field> extends CoValue
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

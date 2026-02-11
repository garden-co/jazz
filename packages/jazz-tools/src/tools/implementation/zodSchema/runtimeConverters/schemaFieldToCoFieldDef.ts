import type { JsonValue } from "cojson";
import {
  type Schema,
  CoValueClass,
  isCoValueClass,
  schemaToRefPermissions,
  getDefaultRefPermissions,
  SchemaPermissions,
  RefPermissions,
  type NewInlineOwnerStrategy,
  type CoreCoDiscriminatedUnionSchema,
  type DiscriminableCoValueSchemas,
  type RefOnCreateCallback,
} from "../../../internal.js";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import {
  isUnionOfPrimitivesDeeply,
  getFlattenedUnionOptions,
  getDiscriminatorValuesForOption,
  resolveDiscriminantValue,
} from "../unionUtils.js";
import {
  ZodCatch,
  ZodDefault,
  ZodLazy,
  ZodReadonly,
  z,
} from "../zodReExport.js";
import { ZodPrimitiveSchema } from "../zodSchema.js";
import { isCoValueSchema } from "./coValueSchemaTransformation.js";

const optionalDateEncoder = {
  encode: (value: Date | undefined) => value?.toISOString() || null,
  decode: (value: JsonValue) =>
    value === null ? undefined : new Date(value as string),
};

/**
 * Types of objects that can be nested inside CoValue schema containers
 */
export type SchemaField =
  // Schemas created with co.map(), co.record(), co.list(), etc.
  | CoreCoValueSchema
  // CoValue classes created with class syntax, or framework-provided classes like Group
  | CoValueClass
  | ZodPrimitiveSchema
  | z.core.$ZodOptional<z.core.$ZodType>
  | z.core.$ZodNullable<z.core.$ZodType>
  | z.core.$ZodUnion<z.core.$ZodType[]>
  | z.core.$ZodDiscriminatedUnion<z.core.$ZodType[]>
  | z.core.$ZodIntersection<z.core.$ZodType, z.core.$ZodType>
  | z.core.$ZodObject<z.core.$ZodLooseShape>
  | z.core.$ZodRecord<z.core.$ZodRecordKey, z.core.$ZodType>
  | z.core.$ZodArray<z.core.$ZodType>
  | z.core.$ZodTuple<z.core.$ZodType[]>
  | z.core.$ZodReadonly<z.core.$ZodType>
  | z.core.$ZodLazy<z.core.$ZodType>
  | z.core.$ZodTemplateLiteral<any>
  | z.core.$ZodLiteral<any>
  | z.core.$ZodEnum<any>
  | z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>
  | z.core.$ZodDefault<z.core.$ZodType>
  | z.core.$ZodCatch<z.core.$ZodType>;

function makeCodecSchema(
  codec: z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>,
): Schema {
  return {
    encoded: {
      encode: (value: any) => {
        if (value === undefined) return undefined as unknown as JsonValue;
        if (value === null) return null;
        return codec._zod.def.reverseTransform(value, {
          value,
          issues: [],
        }) as JsonValue;
      },
      decode: (value) => {
        if (value === null) return null;
        if (value === undefined) return undefined;
        return codec._zod.def.transform(value, { value, issues: [] });
      },
    },
  };
}

const schemaFieldCache = new WeakMap<SchemaField, Schema>();

function cacheSchemaField(schema: SchemaField, value: Schema): Schema {
  schemaFieldCache.set(schema, value);
  return value;
}

const ZOD_JSON_TYPES = new Set([
  "string",
  "number",
  "boolean",
  "null",
  "enum",
  "template_literal",
  "object",
  "record",
  "array",
  "tuple",
  "intersection",
]);

function unsupportedZodTypeError(schema: SchemaField): Error {
  return new Error(
    `Unsupported zod type: ${(schema as any)?._zod?.def?.type || JSON.stringify(schema)}`,
  );
}

function resolveCoSchemaField(
  schema: CoreCoValueSchema & { getCoValueClass: () => CoValueClass },
): Schema {
  return {
    ref: schema.getCoValueClass(),
    optional: schema.builtin === "CoOptional",
    permissions: schemaFieldPermissions(schema),
  };
}

function validateLiteralValues(literals: readonly unknown[]) {
  if (literals.some((literal) => typeof literal === "undefined")) {
    throw new Error("z.literal() with undefined is not supported");
  }
  if (literals.some((literal) => literal === null)) {
    throw new Error("z.literal() with null is not supported");
  }
  if (literals.some((literal) => typeof literal === "bigint")) {
    throw new Error("z.literal() with bigint is not supported");
  }
}

function resolveZodSchemaField(schema: SchemaField): Schema {
  if (!("_zod" in schema)) {
    throw new Error(`Unsupported zod type: ${schema}`);
  }

  const zodSchemaDef = schema._zod.def;

  switch (zodSchemaDef.type) {
    case "optional":
    case "nullable": {
      const inner = zodSchemaDef.innerType as SchemaField;
      const innerZodType = inner as unknown as z.ZodTypeAny;
      if (
        zodSchemaDef.type === "nullable" &&
        innerZodType?._zod?.def?.type === "date"
      ) {
        throw new Error("Nullable z.date() is not supported");
      }
      return resolveSchemaField(inner);
    }

    case "readonly":
      return resolveSchemaField(
        (schema as unknown as ZodReadonly).def.innerType as SchemaField,
      );

    case "date":
      return { encoded: optionalDateEncoder };

    case "lazy":
      // Mostly to support z.json()
      return resolveSchemaField(
        (schema as unknown as ZodLazy).unwrap() as SchemaField,
      );

    case "default":
    case "catch":
      console.warn(
        "z.default()/z.catch() are not supported in collaborative schemas. They will be ignored.",
      );
      return resolveSchemaField(
        (schema as unknown as ZodDefault | ZodCatch).def
          .innerType as SchemaField,
      );

    case "literal":
      validateLiteralValues(zodSchemaDef.values);
      return "json";

    case "union":
      if (!isUnionOfPrimitivesDeeply(schema)) {
        throw new Error(
          "z.union()/z.discriminatedUnion() of collaborative types is not supported. Use co.discriminatedUnion() instead.",
        );
      }
      return "json";

    case "pipe": {
      const isCodec =
        zodSchemaDef.transform !== undefined &&
        zodSchemaDef.reverseTransform !== undefined;

      if (!isCodec) {
        throw new Error(
          "z.pipe() is not supported. Only z.codec() is supported.",
        );
      }

      try {
        resolveSchemaField(zodSchemaDef.in as SchemaField);
      } catch (error) {
        if (error instanceof Error) {
          error.message = `z.codec() is only supported if the input schema is already supported. ${error.message}`;
        }
        throw error;
      }

      return makeCodecSchema(
        schema as z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>,
      );
    }

    default:
      if (ZOD_JSON_TYPES.has(zodSchemaDef.type)) {
        return "json";
      }
      throw unsupportedZodTypeError(schema);
  }
}

export function resolveSchemaField(schema: SchemaField): Schema {
  const cachedSchema = schemaFieldCache.get(schema);
  if (cachedSchema !== undefined) {
    return cachedSchema;
  }

  const resolved = isCoValueClass(schema)
    ? ({
        ref: schema,
        optional: false,
        permissions: getDefaultRefPermissions(),
      } satisfies Schema)
    : isCoValueSchema(schema)
      ? resolveCoSchemaField(
          schema as CoreCoValueSchema & { getCoValueClass: () => CoValueClass },
        )
      : resolveZodSchemaField(schema);

  return cacheSchemaField(schema, resolved);
}

export function schemaFieldToCoFieldDef(schema: SchemaField): Schema {
  return resolveSchemaField(schema);
}

function schemaFieldPermissions(schema: CoreCoValueSchema): RefPermissions {
  if (schema.builtin === "CoOptional") {
    return schemaFieldPermissions((schema as any).innerType);
  }
  if (schema.builtin === "CoDiscriminatedUnion") {
    return discriminatedUnionFieldPermissions(
      schema as CoreCoDiscriminatedUnionSchema<DiscriminableCoValueSchemas>,
    );
  }
  return "permissions" in schema
    ? schemaToRefPermissions(schema.permissions as SchemaPermissions)
    : getDefaultRefPermissions();
}

function discriminatedUnionFieldPermissions(
  schema: CoreCoDiscriminatedUnionSchema<DiscriminableCoValueSchemas>,
): RefPermissions {
  const discriminatorKey = schema.getDefinition().discriminator;
  const allOptions = getFlattenedUnionOptions(schema);

  const valueToStrategy = new Map<unknown, RefPermissions>();
  for (const option of allOptions) {
    const optionPermissions = schemaFieldPermissions(option);
    const discriminatorValues = getDiscriminatorValuesForOption(
      option,
      discriminatorKey,
    );

    if (!discriminatorValues) {
      continue;
    }

    for (const value of discriminatorValues) {
      if (!valueToStrategy.has(value)) {
        valueToStrategy.set(value, optionPermissions);
      }
    }
  }

  const fallbackStrategy = getDefaultRefPermissions();

  const newInlineOwnerStrategy: NewInlineOwnerStrategy = (
    createNewGroup,
    containerOwner,
    init,
  ) => {
    const discriminantValue = resolveDiscriminantValue(init, discriminatorKey);
    const strategy =
      discriminantValue !== undefined
        ? valueToStrategy.get(discriminantValue)
        : undefined;

    const effectiveStrategy = strategy ?? fallbackStrategy;
    return effectiveStrategy.newInlineOwnerStrategy(
      createNewGroup,
      containerOwner,
      init,
    );
  };

  const onCreate: RefOnCreateCallback = (newGroup, init) => {
    const discriminantValue = resolveDiscriminantValue(init, discriminatorKey);
    const strategy =
      discriminantValue !== undefined
        ? valueToStrategy.get(discriminantValue)
        : undefined;

    const effectiveStrategy = strategy ?? fallbackStrategy;
    effectiveStrategy.onCreate?.(newGroup, init);
  };

  return { newInlineOwnerStrategy, onCreate };
}

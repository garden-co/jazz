import type { JsonValue } from "cojson";
import {
  type Schema,
  CoValueClass,
  Encoders,
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

export function resolveSchemaField(schema: SchemaField): Schema {
  const cachedCoFieldDef = schemaFieldCache.get(schema);
  if (cachedCoFieldDef !== undefined) {
    return cachedCoFieldDef;
  }

  if (isCoValueClass(schema)) {
    return cacheSchemaField(schema, {
      ref: schema,
      optional: false,
      permissions: getDefaultRefPermissions(),
    });
  } else if (isCoValueSchema(schema)) {
    if (schema.builtin === "CoOptional") {
      return cacheSchemaField(schema, {
        ref: schema.getCoValueClass(),
        optional: true,
        permissions: schemaFieldPermissions(schema),
      });
    }
    return cacheSchemaField(schema, {
      ref: schema.getCoValueClass(),
      optional: false,
      permissions: schemaFieldPermissions(schema),
    });
  } else {
    if ("_zod" in schema) {
      const zodSchemaDef = schema._zod.def;
      if (
        zodSchemaDef.type === "optional" ||
        zodSchemaDef.type === "nullable"
      ) {
        const inner = zodSchemaDef.innerType as SchemaField;
        const resolved = resolveSchemaField(inner);
        const innerZodType = inner as unknown as z.ZodTypeAny;
        if (
          zodSchemaDef.type === "nullable" &&
          innerZodType?._zod?.def?.type === "date"
        ) {
          throw new Error("Nullable z.date() is not supported");
        }
        return cacheSchemaField(schema, resolved);
      } else if (zodSchemaDef.type === "string") {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "number") {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "boolean") {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "null") {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "enum") {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "readonly") {
        return cacheSchemaField(
          schema,
          resolveSchemaField(
            (schema as unknown as ZodReadonly).def.innerType as SchemaField,
          ),
        );
      } else if (zodSchemaDef.type === "date") {
        return cacheSchemaField(schema, { encoded: Encoders.OptionalDate });
      } else if (zodSchemaDef.type === "template_literal") {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "lazy") {
        // Mostly to support z.json()
        return cacheSchemaField(
          schema,
          resolveSchemaField(
            (schema as unknown as ZodLazy).unwrap() as SchemaField,
          ),
        );
      } else if (
        zodSchemaDef.type === "default" ||
        zodSchemaDef.type === "catch"
      ) {
        console.warn(
          "z.default()/z.catch() are not supported in collaborative schemas. They will be ignored.",
        );

        return cacheSchemaField(
          schema,
          resolveSchemaField(
            (schema as unknown as ZodDefault | ZodCatch).def
              .innerType as SchemaField,
          ),
        );
      } else if (zodSchemaDef.type === "literal") {
        if (
          zodSchemaDef.values.some((literal) => typeof literal === "undefined")
        ) {
          throw new Error("z.literal() with undefined is not supported");
        }
        if (zodSchemaDef.values.some((literal) => literal === null)) {
          throw new Error("z.literal() with null is not supported");
        }
        if (
          zodSchemaDef.values.some((literal) => typeof literal === "bigint")
        ) {
          throw new Error("z.literal() with bigint is not supported");
        }
        return cacheSchemaField(schema, "json");
      } else if (
        zodSchemaDef.type === "object" ||
        zodSchemaDef.type === "record" ||
        zodSchemaDef.type === "array" ||
        zodSchemaDef.type === "tuple" ||
        zodSchemaDef.type === "intersection"
      ) {
        return cacheSchemaField(schema, "json");
      } else if (zodSchemaDef.type === "union") {
        if (isUnionOfPrimitivesDeeply(schema)) {
          return cacheSchemaField(schema, "json");
        } else {
          throw new Error(
            "z.union()/z.discriminatedUnion() of collaborative types is not supported. Use co.discriminatedUnion() instead.",
          );
        }
      } else if (zodSchemaDef.type === "pipe") {
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

        return cacheSchemaField(
          schema,
          makeCodecSchema(
            schema as z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>,
          ),
        );
      } else {
        throw new Error(
          `Unsupported zod type: ${(schema._zod?.def as any)?.type || JSON.stringify(schema)}`,
        );
      }
    } else {
      throw new Error(`Unsupported zod type: ${schema}`);
    }
  }
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

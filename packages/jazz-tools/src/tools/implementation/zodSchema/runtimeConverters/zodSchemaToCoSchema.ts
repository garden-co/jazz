import { RawCoList, RawCoMap } from "cojson";
import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
  SchemaUnion,
  enrichAccountSchema,
  enrichCoDiscriminatedUnionSchema,
  enrichCoFeedSchema,
  enrichCoListSchema,
  enrichCoMapSchema,
  enrichFileStreamSchema,
  enrichPlainTextSchema,
  isCoValueClass,
} from "../../../internal.js";
import { coField } from "../../schema.js";
import { isAnyCoOptionalSchema } from "../schemaTypes/CoOptionalSchema.js";
import { enrichRichTextSchema } from "../schemaTypes/RichTextSchema.js";
import {
  isUnionOfCoMapsDeeply,
  schemaUnionDiscriminatorFor,
} from "../unionUtils.js";
import { z } from "../zodReExport.js";
import {
  AnyCoSchema,
  CoValueClassFromAnySchema,
  CoValueOrZodSchema,
  CoValueSchemaFromZodSchema,
  ZodPrimitiveSchema,
  getDef,
  isZodArray,
  isZodCustom,
  isZodObject,
} from "../zodSchema.js";
import { schemaFieldToCoFieldDef } from "./zodFieldToCoFieldDef.js";

let coSchemasForZodSchemas = new Map<z.core.$ZodType, AnyCoSchema>();

export function isAnyCoValueSchema(
  schema: z.core.$ZodType | CoValueClass,
): schema is AnyCoSchema {
  return "collaborative" in schema && schema.collaborative === true;
}

export function isCoValueSchema(
  schema: z.core.$ZodType | CoValueClass,
): schema is CoValueSchemaFromZodSchema<AnyCoSchema> {
  return isAnyCoValueSchema(schema) && "getCoValueClass" in schema;
}

/**
 * Convert a Zod schema into a CoValue schema.
 *
 * @param schema A Zod schema that may represent a CoValue schema
 * @returns The CoValue schema matching the provided ProtoCoSchema, or `null` if the Zod schema
 * does not match a CoValue schema.
 */
function tryZodSchemaToCoSchema<S extends z.core.$ZodType>(
  schema: S,
): CoValueSchemaFromZodSchema<S> | null {
  if (isAnyCoValueSchema(schema)) {
    if (coSchemasForZodSchemas.has(schema)) {
      return coSchemasForZodSchemas.get(
        schema,
      ) as CoValueSchemaFromZodSchema<S>;
    }

    if (isAnyCoOptionalSchema(schema)) {
      // Optional schemas are not supported as top-level schemas
      return null;
    } else if (isZodObject(schema)) {
      const def = getDef(schema);

      const ClassToExtend =
        "builtin" in schema && schema.builtin === "Account" ? Account : CoMap;

      const coValueClass = class ZCoMap extends ClassToExtend {
        constructor(options: { fromRaw: RawCoMap } | undefined) {
          super(options);
          for (const [field, fieldType] of Object.entries(
            def.shape as z.core.$ZodShape,
          )) {
            (this as any)[field] = schemaFieldToCoFieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(fieldType),
            );
          }
          if (def.catchall) {
            (this as any)[coField.items] = schemaFieldToCoFieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(def.catchall),
            );
          }
        }
      };

      const coValueSchema =
        ClassToExtend === Account
          ? enrichAccountSchema(schema as any, coValueClass as any)
          : enrichCoMapSchema(schema as any, coValueClass as any);

      coSchemasForZodSchemas.set(schema, coValueSchema);
      return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
    } else if (isZodArray(schema)) {
      const def = getDef(schema);
      const coValueClass = class ZCoList extends CoList {
        constructor(options: { fromRaw: RawCoList } | undefined) {
          super(options);
          (this as any)[coField.items] = schemaFieldToCoFieldDef(
            zodSchemaToCoSchemaOrKeepPrimitive(def.element),
          );
        }
      };

      const coValueSchema = enrichCoListSchema(schema, coValueClass as any);

      coSchemasForZodSchemas.set(schema, coValueSchema);
      return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
    } else if (isZodCustom(schema)) {
      if ("builtin" in schema) {
        if (schema.builtin === "CoFeed" && "element" in schema) {
          const coValueClass = CoFeed.Of(
            schemaFieldToCoFieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(
                schema.element as z.core.$ZodType,
              ),
            ),
          );
          const coValueSchema = enrichCoFeedSchema(schema, coValueClass as any);
          return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
        } else if (schema.builtin === "FileStream") {
          const coValueClass = FileStream;
          const coValueSchema = enrichFileStreamSchema(schema, coValueClass);
          return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
        } else if (schema.builtin === "CoPlainText") {
          const coValueClass = CoPlainText;
          const coValueSchema = enrichPlainTextSchema(schema, coValueClass);
          return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
        } else if (schema.builtin === "CoRichText") {
          const coValueClass = CoRichText;
          const coValueSchema = enrichRichTextSchema(schema, coValueClass);
          return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
        } else {
          throw new Error(`Unsupported builtin type: ${schema.builtin}`);
        }
      } else {
        throw new Error(`Unsupported custom zod type`);
      }
    } else {
      throw new Error(
        `Unsupported zod CoValue type for top-level schema: ${schema._zod?.def?.type || JSON.stringify(schema, undefined, 2)}`,
      );
    }
  } else if (schema instanceof z.core.$ZodDiscriminatedUnion) {
    if (isUnionOfCoMapsDeeply(schema)) {
      const coValueClass = SchemaUnion.Of(schemaUnionDiscriminatorFor(schema));
      const coValueSchema = enrichCoDiscriminatedUnionSchema(
        schema as any,
        coValueClass as any,
      );
      return coValueSchema as unknown as CoValueSchemaFromZodSchema<S>;
    } else {
      throw new Error(
        "z.discriminatedUnion() of non-collaborative types is not supported as a top-level schema",
      );
    }
  } else {
    return null;
  }
}

export function zodSchemaToCoSchema<S extends z.core.$ZodType | AnyCoSchema>(
  schema: S,
): CoValueSchemaFromZodSchema<S> {
  const coSchema = tryZodSchemaToCoSchema(schema);
  if (!coSchema) {
    throw new Error(
      `Unsupported zod type: ${schema._zod?.def?.type || JSON.stringify(schema)}`,
    );
  }
  return coSchema;
}

// TODO this should be coValueClassOrAnySchemaToCoValueClass
export function anySchemaToCoSchema<S extends CoValueOrZodSchema>(
  schema: S,
): CoValueClassFromAnySchema<S> {
  if (isCoValueClass(schema)) {
    return schema as any;
  } else if (isCoValueSchema(schema)) {
    return schema.getCoValueClass() as any;
  } else if ("def" in schema) {
    const coSchema = tryZodSchemaToCoSchema(
      schema as z.core.$ZodType | AnyCoSchema,
    );
    if (!coSchema) {
      throw new Error(
        `Unsupported zod type: ${(schema.def as any)?.type || JSON.stringify(schema)}`,
      );
    }
    return coSchema.getCoValueClass() as any;
  }

  throw new Error(`Unsupported schema: ${JSON.stringify(schema)}`);
}

export function zodSchemaToCoSchemaOrKeepPrimitive<S extends z.core.$ZodType>(
  schema: S,
): CoValueSchemaFromZodSchema<S> | ZodPrimitiveSchema {
  const coSchema = tryZodSchemaToCoSchema(schema);
  if (!coSchema) {
    return schema as any;
  }
  return coSchema;
}

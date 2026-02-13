import {
  Account,
  Group,
  isCoValue,
  isCoValueSchema,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import type { CoreCoValueSchema } from "./CoValueSchema.js";

type InputSchema =
  | typeof Group
  | typeof Account
  | CoreCoValueSchema
  | z.ZodType
  | z.core.$ZodType;

export function generateValidationSchemaFromItem(item: InputSchema): z.ZodType {
  // item is Group class
  // This is because users can define the schema
  // using Group class instead of GroupSchema
  // e.g. `co.map({ group: Group })` vs `co.map({ group: co.group() })`
  if (item === Group) {
    return z.instanceof(Group);
  }
  // Same as above: `co.map({ account: Account })` vs `co.map({ account: co.account() })`
  if (item === Account) {
    return z.instanceof(Account);
  }

  if (isCoValueSchema(item)) {
    return item.getValidationSchema();
  }

  if (item instanceof z.core.$ZodType) {
    // the following zod types are not supported:
    if (
      // codecs are managed lower level
      (item as z.ZodType).def.type === "pipe"
    ) {
      return z.any();
    }

    return item as z.ZodType;
  }

  throw new Error(`Unsupported schema type: ${item}`);
}

/**
 * Returns a Zod schema that accepts either an instance of the given CoValue class
 * or a plain value valid against the given plain schema. Validation is not used on read,
 * so existing CoValue instances are accepted; for non-CoValue inputs, validation runs
 * against the plain schema. The result includes `plainSchema` in meta for extraction.
 */
export function coValueValidationSchema(
  plainSchema: z.ZodType,
  expectedCoValueClass: new (...args: any[]) => unknown,
): z.ZodType {
  return z
    .unknown()
    .superRefine((value, ctx) => {
      if (isCoValue(value)) {
        if (!(value instanceof expectedCoValueClass)) {
          ctx.addIssue({
            code: "custom",
            message: `Expected a ${expectedCoValueClass.name} when providing a CoValue instance`,
          });
        }
        return;
      }

      const parsedValue = plainSchema.safeParse(value);
      if (!parsedValue.success) {
        for (const issue of parsedValue.error.issues) {
          ctx.addIssue({ ...issue });
        }
      }
    })
    .meta({
      plainSchema,
    });
}

export function extractPlainSchema(schema: z.ZodType): z.ZodType {
  // plainSchema is only set on unknown schemas with superRefine
  if (schema.def.type !== "unknown") {
    return schema;
  }
  const plainSchema = schema.meta()?.plainSchema;
  if (plainSchema) {
    return plainSchema as z.ZodType;
  }

  throw new Error("Schema does not have a plain schema");
}

export function expectObjectSchema(schema: z.ZodType): z.ZodObject {
  if (schema.def.type === "object") {
    return schema as z.ZodObject;
  }

  const plainSchema = extractPlainSchema(schema);
  if (plainSchema.def.type === "object") {
    return plainSchema as z.ZodObject;
  }

  throw new Error("Schema does not have an object schema");
}

export function expectArraySchema(schema: z.ZodType): z.ZodArray<z.ZodType> {
  if (schema.def.type === "array") {
    return schema as z.ZodArray<z.ZodType>;
  }

  const plainSchema = extractPlainSchema(schema);
  if (plainSchema.def.type === "array") {
    return plainSchema as z.ZodArray<z.ZodType>;
  }

  throw new Error("Schema does not have an array schema");
}

export function normalizeZodSchema(schema: z.ZodType): z.ZodType {
  // ignore codecs/pipes
  // even if they are nested into optional and nullable
  if (
    schema.def?.type === "pipe" ||
    // @ts-expect-error
    schema.def?.innerType?.def?.type === "pipe" ||
    // @ts-expect-error
    schema.def?.innerType?.def?.innerType?.def?.type === "pipe"
  ) {
    return z.any();
  }

  return schema;
}

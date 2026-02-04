import { Account, Group, isCoValueSchema } from "../../../internal.js";
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

function isUnionSchema(schema: unknown): schema is z.ZodUnion {
  if (typeof schema !== "object" || schema === null) {
    return false;
  }

  if ("type" in schema && schema.type === "union") {
    return true;
  }

  return false;
}

export function extractFieldShapeFromUnionSchema(schema: unknown): z.ZodObject {
  if (!isUnionSchema(schema)) {
    throw new Error("Schema is not a union");
  }

  const unionElement = schema.options[1];

  if (typeof unionElement !== "object" || unionElement === null) {
    throw new Error("Union element is not an object");
  }

  if ("shape" in unionElement) {
    return unionElement as z.ZodObject;
  }

  throw new Error("Union element is not an object with shape");
}

export function extractFieldElementFromUnionSchema(schema: unknown): z.ZodType {
  if (!isUnionSchema(schema)) {
    throw new Error("Schema is not a union");
  }

  const unionElement = schema.options[1];

  if (typeof unionElement !== "object" || unionElement === null) {
    throw new Error("Union element is not an object");
  }

  if ("element" in unionElement) {
    return unionElement.element as z.ZodType;
  }

  throw new Error("Union element is not an object with element");
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

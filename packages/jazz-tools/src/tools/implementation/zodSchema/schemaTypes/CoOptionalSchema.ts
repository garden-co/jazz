import { isAnyCoValueSchema } from "../runtimeConverters/zodSchemaToCoSchema.js";
import { z } from "../zodReExport.js";
import { AnyCoSchema, CoValueSchemaFromZodSchema } from "../zodSchema.js";

export type AnyCoOptionalSchema<
  Shape extends z.core.$ZodType = z.core.$ZodType,
> = z.ZodOptional<Shape> & {
  collaborative: true;
};

export type CoOptionalSchema<Shape extends z.core.$ZodType = z.core.$ZodType> =
  AnyCoOptionalSchema<Shape> & {
    getCoValueClass: () => CoValueSchemaFromZodSchema<AnyCoSchema>["getCoValueClass"];
  };

export function createCoOptionalSchema<T extends AnyCoSchema>(
  schema: T,
): CoOptionalSchema<T> {
  return Object.assign(z.optional(schema), {
    collaborative: true,
    getCoValueClass: () => {
      return (
        schema as CoValueSchemaFromZodSchema<AnyCoSchema>
      ).getCoValueClass();
    },
  }) as unknown as CoOptionalSchema<T>;
}

export function isAnyCoOptionalSchema(
  schema: z.core.$ZodType,
): schema is CoOptionalSchema<z.core.$ZodType> {
  return isAnyCoValueSchema(schema) && schema._zod.def.type === "optional";
}

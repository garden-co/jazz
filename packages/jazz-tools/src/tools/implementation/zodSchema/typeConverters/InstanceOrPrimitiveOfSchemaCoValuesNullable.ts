import {
  AnyZodOrCoValueSchema,
  CoValueClass,
  InstanceOfSchemaCoValuesNullable,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { TypeOfZodSchema } from "./TypeOfZodSchema.js";

export type InstanceOrPrimitiveOfSchemaCoValuesNullable<
  S extends CoValueClass | AnyZodOrCoValueSchema,
> = S extends z.core.$ZodType
  ? TypeOfZodSchema<S>
  : InstanceOfSchemaCoValuesNullable<S>;

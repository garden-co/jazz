import {
  AnyZodOrCoValueSchema,
  CoValueClass,
  InstanceOfSchema,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { TypeOfZodSchema } from "./TypeOfZodSchema.js";

/**
 * A loaded CoValue or primitive type.
 * If it's a CoValue, its references to other CoValues are also loaded.
 */
export type InstanceOrPrimitiveOfSchema<S> = S extends z.core.$ZodType
  ? TypeOfZodSchema<S>
  : InstanceOfSchema<S>;

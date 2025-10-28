import {
  AnyZodOrCoValueSchema,
  CoValueClass,
  InstanceOfSchema,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { TypeOfZodSchema } from "./TypeOfZodSchema.js";

export type InstanceOrPrimitiveOfSchema<
  S extends CoValueClass | AnyZodOrCoValueSchema,
> = S extends z.core.$ZodType ? TypeOfZodSchema<S> : InstanceOfSchema<S>;

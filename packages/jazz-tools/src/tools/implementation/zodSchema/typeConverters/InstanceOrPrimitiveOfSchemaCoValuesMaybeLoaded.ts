import {
  AnyZodOrCoValueSchema,
  CoValueClass,
  InstanceOfSchemaCoValuesMaybeLoaded,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { TypeOfZodSchema } from "./TypeOfZodSchema.js";

/**
 * A loaded CoValue or primitive type.
 * If it's a CoValue, its references to other CoValues may or may not be loaded.
 */
export type InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<
  S extends CoValueClass | AnyZodOrCoValueSchema,
> = S extends z.core.$ZodType
  ? TypeOfZodSchema<S>
  : InstanceOfSchemaCoValuesMaybeLoaded<S>;

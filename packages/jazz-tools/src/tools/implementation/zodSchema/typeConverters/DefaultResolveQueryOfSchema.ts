import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";

/**
 * Get the default resolve query of a CoValue schema.
 */
export type DefaultResolveQueryOfSchema<S extends AnyZodOrCoValueSchema> =
  S extends CoreCoValueSchema ? S["defaultResolveQuery"] : false;

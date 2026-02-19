import { z } from "../zodReExport.js";

/**
 * "Core" CoValue schemas contain all data necessary to represent a CoValue schema.
 * Behavior is provided by CoValue schemas that extend "core" CoValue schema data structures.
 *
 * "Core" CoValue schemas are necessary to avoid circularity issues when defining schemas.
 * This is similar to how Zod's "core" schemas are used.
 *
 * They are not meant to be used directly outside of the `jazz-tools` package,
 * use concrete CoValue schemas instead.
 */
export interface CoreCoValueSchema {
  collaborative: true;

  /**
   * Used for discriminating between different CoValue schemas.
   */
  builtin: string;

  resolveQuery: CoreResolveQuery;

  getValidationSchema: () => z.ZodType;
}

/**
 * Simplified type for {@link ResolveQuery}, used to avoid circularity issues.
 */
export type CoreResolveQuery = boolean | Record<string, any>;

import type { CoreAccountSchema } from "./schemaTypes/AccountSchema.js";
import type { CoreCoFeedSchema } from "./schemaTypes/CoFeedSchema.js";
import type { CoreCoListSchema } from "./schemaTypes/CoListSchema.js";
import type { CoreCoMapSchema } from "./schemaTypes/CoMapSchema.js";
import type { CoreCoValueSchema } from "./schemaTypes/CoValueSchema.js";

type ConstructorWithSchema<S extends CoreCoValueSchema = CoreCoValueSchema> = {
  name?: string;
  coValueSchema?: S;
};

export function assertCoreCoValueSchema<C extends ConstructorWithSchema>(
  constructor: C,
  operation: "create" | "load" | "resolve",
): NonNullable<C["coValueSchema"]> {
  const schema = constructor.coValueSchema;
  if (!schema) {
    const className = constructor.name || "AnonymousCoValue";
    throw new Error(
      `[schema-invariant] ${className}.${operation} requires a coValueSchema. ` +
        `Attach a schema via co.map/co.list/co.feed/co.account before using this class.`,
    );
  }
  return schema as NonNullable<C["coValueSchema"]>;
}

type CoValueSchema =
  | CoreCoMapSchema
  | CoreCoListSchema
  | CoreCoFeedSchema
  | CoreAccountSchema;

export function assertCoValueSchema<
  T extends CoValueSchema["builtin"],
  C extends ConstructorWithSchema,
>(
  constructor: C,
  type: T,
  operation: "create" | "load" | "resolve",
): Extract<CoValueSchema, { builtin: T }> {
  const schema = assertCoreCoValueSchema(constructor, operation);

  if (schema.builtin !== type) {
    const className = constructor.name || "AnonymousCoValue";

    throw new Error(
      `[schema-invariant] ${className}.${operation} requires a ${type} schema. ` +
        `Attached schema is ${schema.builtin}.`,
    );
  }

  return schema as Extract<CoValueSchema, { builtin: T }>;
}

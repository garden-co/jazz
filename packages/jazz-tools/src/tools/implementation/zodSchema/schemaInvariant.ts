import type { CoreCoFeedSchema } from "./schemaTypes/CoFeedSchema.js";
import type { CoreCoListSchema } from "./schemaTypes/CoListSchema.js";
import type { CoreCoMapSchema } from "./schemaTypes/CoMapSchema.js";
import type { CoreCoValueSchema } from "./schemaTypes/CoValueSchema.js";

type ConstructorLike = {
  name?: string;
  coValueSchema?: CoreCoValueSchema;
};

export function assertCoreCoValueSchema(
  constructor: unknown,
  operation: "create" | "load" | "resolve",
): CoreCoValueSchema {
  const constructorLike = constructor as ConstructorLike;
  const schema = constructorLike.coValueSchema;

  if (!schema) {
    const className = constructorLike.name || "AnonymousCoValue";
    throw new Error(
      `[schema-invariant] ${className}.${operation} requires a coValueSchema. ` +
        `Attach a schema via co.map/co.list/co.feed/co.account before using this class.`,
    );
  }

  return schema;
}

type CoValueSchema = CoreCoMapSchema | CoreCoListSchema | CoreCoFeedSchema;

export function assertCoValueSchema<T extends CoValueSchema["builtin"]>(
  constructor: unknown,
  type: T,
  operation: "create" | "load" | "resolve",
): Extract<CoValueSchema, { builtin: T }> {
  const schema = assertCoreCoValueSchema(constructor, operation);

  if (schema.builtin !== type) {
    throw new Error(
      `[schema-invariant] ${(constructor as ConstructorLike).name}.${operation} requires a ${type} schema. ` +
        `Attached schema is ${schema.builtin}.`,
    );
  }

  return schema as Extract<CoValueSchema, { builtin: T }>;
}

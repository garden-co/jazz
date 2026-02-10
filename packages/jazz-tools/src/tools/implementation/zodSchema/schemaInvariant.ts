import type { CoreCoValueSchema } from "./schemaTypes/CoValueSchema.js";

export function assertCoValueSchema(
  constructor: unknown,
  operation: "create" | "load" | "resolve",
): CoreCoValueSchema {
  const constructorLike = constructor as {
    name?: string;
    coValueSchema?: CoreCoValueSchema;
  };
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

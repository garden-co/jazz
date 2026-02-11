import type { CoreCoValueSchema } from "./schemaTypes/CoValueSchema.js";

type ConstructorWithSchema<S extends CoreCoValueSchema = CoreCoValueSchema> = {
  name?: string;
  coValueSchema?: S;
};

export function assertCoValueSchema<C extends ConstructorWithSchema>(
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

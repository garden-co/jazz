import { CoValueSchema } from "../types.js";

export const LazySchemaSymbol = "LazySchema" as const;

export class LazySchema<T extends CoValueSchema> {
  lazySchema: () => T;
  [LazySchemaSymbol]: true;

  declare _schema: T;

  constructor(lazySchema: () => T) {
    this.lazySchema = lazySchema;
    this[LazySchemaSymbol] = true;
  }
}

export function lazy<T extends CoValueSchema>(
  lazySchema: () => T,
): LazySchema<T> {
  return new LazySchema(lazySchema);
}

export function isLazySchema<T extends CoValueSchema>(
  value: unknown,
): value is LazySchema<T> {
  return (
    typeof value === "object" && value !== null && LazySchemaSymbol in value
  );
}

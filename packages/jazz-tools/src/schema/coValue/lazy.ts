import { CoValueSchema } from "../coMap/schema.js";

export const LazySchemaSymbol = "LazySchema" as const;

export type LazySchema<T> = {
  [LazySchemaSymbol]: true;
  lazySchema: () => T;
};

export function lazy<T extends CoValueSchema>(
  lazySchema: () => T,
): LazySchema<T> {
  return {
    [LazySchemaSymbol]: true,
    lazySchema,
  };
}

export function isLazySchema<T extends CoValueSchema>(
  value: unknown,
): value is LazySchema<T> {
  return (
    typeof value === "object" && value !== null && LazySchemaSymbol in value
  );
}

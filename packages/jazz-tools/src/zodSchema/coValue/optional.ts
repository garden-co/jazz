import { CoValueSchema } from "../coMap/schema.js";
import { LazySchema, isLazySchema } from "./lazy.js";

export const OptionalSymbol = "isOptional" as const;

export function optional<T extends { optional: () => any }>(
  value: T,
): T extends { optional: () => infer O } ? O : never {
  return value.optional();
}

export function isOptional<T extends CoValueSchema | LazySchema<any>>(
  value: T,
) {
  if (isLazySchema(value)) {
    return isOptional(value.lazySchema());
  }

  return (value as CoValueSchema).isOptional;
}

export type Optional<T extends { _optionalType: any }> = T["_optionalType"];

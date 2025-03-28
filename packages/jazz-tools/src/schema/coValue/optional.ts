import { ZodTypeAny } from "zod";
import { CoValueSchema } from "../coMap/schema.js";
import { LazySchema, isLazySchema } from "./lazy.js";

export const OptionalSymbol = "isOptional" as const;

export type isOptional<T> = T extends {
  isOptional: true;
}
  ? true
  : false;

export type addOptional<T> = isOptional<T> extends true ? undefined : never;

export function optional<T extends { optional: () => any }>(
  value: T,
): T extends { optional: () => infer O } ? O : never {
  return value.optional();
}

export function isOptional<
  T extends CoValueSchema | LazySchema<any> | ZodTypeAny,
>(value: T): value is T & { isOptional: true } {
  if (isLazySchema(value)) {
    return isOptional(value.lazySchema());
  }

  return (value as CoValueSchema).isOptional;
}

export type Optional<T extends CoValueSchema> = ReturnType<T["optional"]>;

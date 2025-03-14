import { CoValueSchema } from "../coMap/schema.js";
import { SelfReference } from "./self.js";

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

export function isOptional<T extends CoValueSchema | SelfReference>(
  value: T,
): value is T & { isOptional: true } {
  return value.isOptional;
}

import { ZodError } from "zod";
import { CoValueSchema, CoValueSchemaToClass } from "../coMap/schema.js";
import { ID, Unloaded, UnloadedJazzAPI } from "./types.js";

type LoadingErrors = ZodError;

export function getUnloadedJazzAPI<
  D extends CoValueSchema,
  E extends LoadingErrors,
>(schema: D, value: ID<D>, state: Unloaded<D>["$jazzState"], error?: E) {
  return Object.defineProperties<Unloaded<D>>({} as Unloaded<D>, {
    $jazzState: {
      value: state,
      writable: false,
      enumerable: true,
      configurable: false,
    },
    $jazz: {
      value: {
        schema: schema as CoValueSchemaToClass<D>,
        id: value,
        error,
      },
      writable: false,
      enumerable: false,
      configurable: false,
    },
  });
}

export function getUnloadedState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
) {
  return getUnloadedJazzAPI(schema, value, "unloaded");
}

export function getUnauthorizedState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
  error?: ZodError,
) {
  return getUnloadedJazzAPI(schema, value, "unauthorized", error);
}

export function getUnavailableState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
  error?: ZodError,
) {
  return getUnloadedJazzAPI(schema, value, "unavailable", error);
}

export function getValidationErrorState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
  error: ZodError,
) {
  return getUnloadedJazzAPI(schema, value, "validationError", error);
}

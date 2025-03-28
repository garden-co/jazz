import { CoValueSchema, CoValueSchemaToClass } from "../coMap/schema.js";
import { ID, Unloaded, UnloadedJazzAPI } from "./types.js";

function getUnloadedJazzAPI<D extends CoValueSchema>(schema: D, value: ID<D>) {
  return {
    schema: schema as CoValueSchemaToClass<D>,
    id: value,
  } as UnloadedJazzAPI<D>;
}

export function getUnloadedState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
) {
  return {
    $jazzState: "unloaded" as const,
    $jazz: getUnloadedJazzAPI(schema, value),
  } satisfies Unloaded<D>;
}

export function getUnauthorizedState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
) {
  return {
    $jazzState: "unauthorized" as const,
    $jazz: getUnloadedJazzAPI(schema, value),
  } satisfies Unloaded<D>;
}

export function getUnavailableState<D extends CoValueSchema>(
  schema: D,
  value: ID<D>,
) {
  return {
    $jazzState: "unavailable" as const,
    $jazz: getUnloadedJazzAPI(schema, value),
  } satisfies Unloaded<D>;
}

import type { QueryOptions } from "../runtime/db.js";

// This capability is deliberately package-private. The Inspector bundle imports
// it from the workspace while it is built; published applications have no
// `jazz-tools` export path to this module and cannot mint local-only reads.
const INSPECTOR_LOCAL_READ_CAPABILITY = Symbol("jazz.inspectorLocalRead");

/** @internal Build-time integration for the private Inspector bundle only. */
export function createInspectorLocalQueryOptions(
  options: Omit<QueryOptions, "tier"> = {},
): QueryOptions {
  const capabilityOptions: QueryOptions = { ...options };
  Object.defineProperty(capabilityOptions, INSPECTOR_LOCAL_READ_CAPABILITY, {
    value: true,
    enumerable: false,
    configurable: false,
    writable: false,
  });
  return Object.freeze(capabilityOptions);
}

/** @internal Runtime check paired with the private Inspector capability. */
export function isInspectorLocalQueryOptions(options?: QueryOptions): boolean {
  return (
    (options as { [INSPECTOR_LOCAL_READ_CAPABILITY]?: unknown } | undefined)?.[
      INSPECTOR_LOCAL_READ_CAPABILITY
    ] === true
  );
}

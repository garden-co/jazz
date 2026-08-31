import { createInspectorLocalQueryOptions, type QueryOptions } from "../runtime/db.js";

/**
 * Options capability for the Inspector overlay's offline, local-only reads.
 *
 * Application queries use the normal `QueryOptions` surface.  The Inspector
 * imports this from `jazz-tools/dev` because it deliberately observes the
 * host's local state without subscribing that inspection query upstream.
 */
export function inspectorLocalQueryOptions(options?: Omit<QueryOptions, "tier">): QueryOptions {
  return createInspectorLocalQueryOptions(options);
}

import type { DbConfig } from "jazz-tools";
import type { InspectorRuntimeContext } from "./host-link";

/**
 * A reload has no user-selected context to restore. Prefer the runtime that
 * published the host handle rather than treating registration order as an
 * identity: sibling providers can register in either order (notably under
 * React StrictMode), and an inspector opened for the host should reconnect to
 * that host's own persistent context by default.
 */
export function defaultRuntimeContextKey(
  contexts: InspectorRuntimeContext[],
  hostConfig: Pick<DbConfig, "appId" | "runtimeSources"> | null,
): string | null {
  // The host derives this in its own realm, where it has the verified session
  // capability necessary to resolve reserved-issuer auth scope. Two accounts
  // can intentionally share an app and logical base while owning distinct
  // physical contexts, so require exact equality here.
  const hostPhysicalDbName = hostConfig?.runtimeSources?.inspectorHostPhysicalDbName ?? null;
  return (
    contexts.find(
      (context) => context.appId === hostConfig?.appId && context.dbName === hostPhysicalDbName,
    )?.key ??
    contexts[0]?.key ??
    null
  );
}

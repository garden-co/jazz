import type { InspectorSubscription, StoredPermissionsResponse, WasmSchema } from "jazz-tools";
import { createContext, useContext, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "overlay";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  storedPermissions: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  /**
   * The host app's active subscriptions (overlay only), pushed from the host
   * window. Empty for the standalone build, which polls server introspection.
   */
  hostSubscriptions: InspectorSubscription[];
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
  storedPermissions = null,
  runtime,
  hostSubscriptions = [],
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  storedPermissions?: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  hostSubscriptions?: InspectorSubscription[];
}>) {
  return (
    <DevtoolsContext.Provider value={{ wasmSchema, storedPermissions, runtime, hostSubscriptions }}>
      {children}
    </DevtoolsContext.Provider>
  );
}

export function useDevtoolsContext(): DevtoolsContextValue {
  const context = useContext(DevtoolsContext);
  if (!context) {
    throw new Error("useDevtoolsContext must be used inside DevtoolsContext");
  }
  return context;
}

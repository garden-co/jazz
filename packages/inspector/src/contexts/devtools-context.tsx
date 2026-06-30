import type {
  InspectorSubscription,
  QueryPropagation,
  StoredPermissionsResponse,
  WasmSchema,
} from "jazz-tools";
import { createContext, useContext, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "overlay";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  storedPermissions: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  /**
   * True only when the inspector is rendered inside the dev-overlay iframe.
   * Gates overlay-specific UI (Close button, launcher-hide setting).
   */
  isOverlay: boolean;
  /**
   * The host app's active subscriptions (overlay only), pushed from the host
   * window. Empty for the standalone build, which polls server introspection.
   */
  hostSubscriptions: InspectorSubscription[];
  /** Both runtimes are server-backed, so propagation is always "full". */
  queryPropagation: QueryPropagation;
  setQueryPropagation: (value: QueryPropagation) => void;
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
  storedPermissions = null,
  runtime,
  isOverlay = false,
  hostSubscriptions = [],
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  storedPermissions?: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  isOverlay?: boolean;
  hostSubscriptions?: InspectorSubscription[];
}>) {
  return (
    <DevtoolsContext.Provider
      value={{
        wasmSchema,
        storedPermissions,
        runtime,
        isOverlay,
        hostSubscriptions,
        queryPropagation: "full",
        setQueryPropagation: () => {},
      }}
    >
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

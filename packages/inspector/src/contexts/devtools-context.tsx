import type { QueryPropagation, StoredPermissionsResponse, WasmSchema } from "jazz-tools";
import { createContext, useContext, useMemo, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "extension" | "overlay";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  storedPermissions: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  /**
   * True only when the inspector is rendered inside the dev-overlay iframe.
   * Gates overlay-specific UI (Close button, launcher-hide setting).
   */
  isOverlay: boolean;
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
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  storedPermissions?: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  isOverlay?: boolean;
}>) {
  const value = useMemo(
    () => ({
      wasmSchema,
      storedPermissions,
      runtime,
      isOverlay,
      queryPropagation: "full" as const,
      setQueryPropagation: () => {},
    }),
    [wasmSchema, storedPermissions, runtime, isOverlay],
  );
  return <DevtoolsContext.Provider value={value}>{children}</DevtoolsContext.Provider>;
}

export function useDevtoolsContext(): DevtoolsContextValue {
  const context = useContext(DevtoolsContext);
  if (!context) {
    throw new Error("useDevtoolsContext must be used inside DevtoolsContext");
  }
  return context;
}

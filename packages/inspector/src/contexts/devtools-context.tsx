import type { QueryPropagation, StoredPermissionsResponse, WasmSchema } from "jazz-tools";
import { createContext, useContext, useState, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "extension";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  storedPermissions: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  /**
   * True only when the inspector is rendered inside the dev-overlay iframe (as
   * opposed to the standalone app or the browser-extension panel). Gates
   * overlay-specific UI such as the launcher-button setting. Kept separate from
   * `runtime` so it doesn't disturb the runtime branches, which treat the
   * overlay as "extension".
   */
  isOverlay: boolean;
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
  queryPropagation,
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  storedPermissions?: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  isOverlay?: boolean;
  queryPropagation?: QueryPropagation;
}>) {
  const [extensionQueryPropagation, setExtensionQueryPropagation] = useState<QueryPropagation>(
    queryPropagation ?? "local-only",
  );
  const resolvedPropagation = runtime === "standalone" ? "full" : extensionQueryPropagation;
  const setQueryPropagation = (value: QueryPropagation) => {
    if (runtime === "standalone") return;
    setExtensionQueryPropagation(value);
  };

  return (
    <DevtoolsContext.Provider
      value={{
        wasmSchema,
        storedPermissions,
        runtime,
        isOverlay,
        queryPropagation: resolvedPropagation,
        setQueryPropagation,
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

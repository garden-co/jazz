import type { QueryPropagation, StoredPermissionsResponse, WasmSchema } from "jazz-tools";
import { createContext, useContext, useState, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "extension";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  storedPermissions: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
  queryPropagation: QueryPropagation;
  setQueryPropagation: (value: QueryPropagation) => void;
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
  storedPermissions = null,
  runtime,
  queryPropagation,
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  storedPermissions?: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
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

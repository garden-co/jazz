import type { StoredPermissionsResponse, WasmSchema } from "jazz-tools";
import { createContext, useContext, useMemo, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "overlay";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  storedPermissions: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
  storedPermissions = null,
  runtime,
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  storedPermissions?: StoredPermissionsResponse | null;
  runtime: InspectorRuntime;
}>) {
  const value = useMemo(
    () => ({ wasmSchema, storedPermissions, runtime }),
    [wasmSchema, storedPermissions, runtime],
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

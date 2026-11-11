import type { PublishedBundle, TablePolicies, WasmSchema } from "jazz-tools";
import { createContext, useContext, useMemo, type PropsWithChildren } from "react";

// "backend": served by an app's backend at `<basePath>/inspect/`, reading through its Inspector API.
export type InspectorRuntime = "standalone" | "overlay" | "backend";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  /** The latest published bundle for the inspected schema. */
  bundle: PublishedBundle | null;
  /** For the "backend" runtime: the permissions the backend's own code runs, per table. */
  backendPermissions: Record<string, TablePolicies> | null;
  runtime: InspectorRuntime;
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
  bundle = null,
  backendPermissions = null,
  runtime,
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  bundle?: PublishedBundle | null;
  backendPermissions?: Record<string, TablePolicies> | null;
  runtime: InspectorRuntime;
}>) {
  const value = useMemo(
    () => ({ wasmSchema, bundle, backendPermissions, runtime }),
    [wasmSchema, bundle, backendPermissions, runtime],
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

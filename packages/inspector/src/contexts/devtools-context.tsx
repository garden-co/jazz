import type { PublishedBundle, WasmSchema } from "jazz-tools";
import { createContext, useContext, useMemo, type PropsWithChildren } from "react";

export type InspectorRuntime = "standalone" | "overlay";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
  /** The latest published bundle for the inspected schema. */
  bundle: PublishedBundle | null;
  runtime: InspectorRuntime;
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
  bundle = null,
  runtime,
}: PropsWithChildren<{
  wasmSchema: WasmSchema;
  bundle?: PublishedBundle | null;
  runtime: InspectorRuntime;
}>) {
  const value = useMemo(() => ({ wasmSchema, bundle, runtime }), [wasmSchema, bundle, runtime]);
  return <DevtoolsContext.Provider value={value}>{children}</DevtoolsContext.Provider>;
}

export function useDevtoolsContext(): DevtoolsContextValue {
  const context = useContext(DevtoolsContext);
  if (!context) {
    throw new Error("useDevtoolsContext must be used inside DevtoolsContext");
  }
  return context;
}

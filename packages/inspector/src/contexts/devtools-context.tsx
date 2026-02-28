import type { WasmSchema } from "jazz-tools";
import { createContext, useContext, type PropsWithChildren } from "react";

interface DevtoolsContextValue {
  wasmSchema: WasmSchema;
}

export const DevtoolsContext = createContext<DevtoolsContextValue | null>(null);

export function DevtoolsProvider({
  children,
  wasmSchema,
}: PropsWithChildren<{ wasmSchema: WasmSchema }>) {
  return <DevtoolsContext.Provider value={{ wasmSchema }}>{children}</DevtoolsContext.Provider>;
}

export function useDevtoolsContext(): DevtoolsContextValue {
  const context = useContext(DevtoolsContext);
  if (!context) {
    throw new Error("useDevtoolsContext must be used inside DevtoolsContext");
  }
  return context;
}

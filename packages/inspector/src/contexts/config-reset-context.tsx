import { createContext, useContext, type PropsWithChildren } from "react";

interface ConfigResetContextValue {
  onReset: () => void;
}

const ConfigResetContext = createContext<ConfigResetContextValue | null>(null);

export function ConfigResetProvider({
  children,
  onReset,
}: PropsWithChildren<{ onReset: () => void }>) {
  return <ConfigResetContext.Provider value={{ onReset }}>{children}</ConfigResetContext.Provider>;
}

export function useConfigReset(): ConfigResetContextValue | null {
  return useContext(ConfigResetContext);
}

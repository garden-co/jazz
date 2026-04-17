import { createContext, useContext, type PropsWithChildren } from "react";

export interface StandaloneConnectionConfig {
  serverUrl: string;
  appId: string;
  adminSecret: string;
  serverPathPrefix?: string;
}

interface StandaloneContextValue {
  onEdit: () => void;
  onReset: () => void;
  schemaHashes: string[];
  selectedSchemaHash: string | null;
  onSelectSchema: (schemaHash: string) => void;
  isSwitchingSchema: boolean;
  connection: StandaloneConnectionConfig;
}

const StandaloneContext = createContext<StandaloneContextValue | null>(null);

export function StandaloneProvider({
  children,
  onEdit,
  onReset,
  schemaHashes,
  selectedSchemaHash,
  onSelectSchema,
  isSwitchingSchema,
  connection,
}: PropsWithChildren<StandaloneContextValue>) {
  return (
    <StandaloneContext.Provider
      value={{
        onEdit,
        onReset,
        schemaHashes,
        selectedSchemaHash,
        onSelectSchema,
        isSwitchingSchema,
        connection,
      }}
    >
      {children}
    </StandaloneContext.Provider>
  );
}

export function useStandaloneContext(): StandaloneContextValue | null {
  return useContext(StandaloneContext);
}

import { createContext, useContext, type PropsWithChildren } from "react";

export interface StandaloneConnectionConfig {
  serverUrl: string;
  appId: string;
  adminSecret: string;
}

interface StandaloneContextValue {
  onManageConnections: () => void;
  schemaHashes: string[];
  selectedSchemaHash: string | null;
  onSelectSchema: (schemaHash: string) => void;
  isSwitchingSchema: boolean;
  connection: StandaloneConnectionConfig;
}

const StandaloneContext = createContext<StandaloneContextValue | null>(null);

export function StandaloneProvider({
  children,
  onManageConnections,
  schemaHashes,
  selectedSchemaHash,
  onSelectSchema,
  isSwitchingSchema,
  connection,
}: PropsWithChildren<StandaloneContextValue>) {
  return (
    <StandaloneContext.Provider
      value={{
        onManageConnections,
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

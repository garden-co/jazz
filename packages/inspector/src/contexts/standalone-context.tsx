import { createContext, useContext, type PropsWithChildren } from "react";

interface StandaloneContextValue {
  onReset: () => void;
  schemaHashes: string[];
  selectedSchemaHash: string | null;
  onSelectSchema: (schemaHash: string) => void;
  isSwitchingSchema: boolean;
}

const StandaloneContext = createContext<StandaloneContextValue | null>(null);

export function StandaloneProvider({
  children,
  onReset,
  schemaHashes,
  selectedSchemaHash,
  onSelectSchema,
  isSwitchingSchema,
}: PropsWithChildren<StandaloneContextValue>) {
  return (
    <StandaloneContext.Provider
      value={{ onReset, schemaHashes, selectedSchemaHash, onSelectSchema, isSwitchingSchema }}
    >
      {children}
    </StandaloneContext.Provider>
  );
}

export function useStandaloneContext(): StandaloneContextValue | null {
  return useContext(StandaloneContext);
}

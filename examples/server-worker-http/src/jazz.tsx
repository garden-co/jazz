import { JazzReactProvider } from "jazz-tools/react";
import { apiKey } from "./apiKey";
import { JazzInspector } from "jazz-tools/inspector";

export function Jazz({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      enableSSR
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <JazzInspector />
      {children}
    </JazzReactProvider>
  );
}

import { JazzReactProvider } from "jazz-tools/react";

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co",
      }}
    >
      {children}
    </JazzReactProvider>
  );
}

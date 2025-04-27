import { JazzProvider } from "jazz-react";

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co",
      }}
    >
      {children}
    </JazzProvider>
  );
}

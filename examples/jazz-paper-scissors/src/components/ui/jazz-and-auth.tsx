import { JazzProvider } from "jazz-react";
import { Account } from "jazz-tools";

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      AccountSchema={Account}
      sync={{
        peer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co",
      }}
    >
      {children}
    </JazzProvider>
  );
}

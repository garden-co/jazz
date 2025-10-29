import * as React from "react";
import { JazzReactProvider } from "jazz-tools/react";

// #region Basic
export function MyApp({ children }: { children: React.ReactNode }) {
  // Get a free API Key at dashboard.jazz.tools, or use your email as a temporary key.
  const apiKey = "you@example.com";
  return (
    <JazzReactProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        // ...
      }}
    >
      {children}
    </JazzReactProvider>
  );
}
// #endregion

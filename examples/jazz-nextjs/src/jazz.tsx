"use client";

import { JazzInspector } from "jazz-inspector";
import { JazzProvider } from "jazz-react";

export function Jazz({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      experimental_enableSSR
      sync={{
        peer: `wss://cloud.jazz.tools/`,
      }}
    >
      {children}
      <JazzInspector />
    </JazzProvider>
  );
}

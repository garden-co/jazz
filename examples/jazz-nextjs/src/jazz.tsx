"use client";

import { JazzInspector } from "jazz-inspector";
import { JazzProvider } from "jazz-react";
import { Account } from "jazz-tools";

export function Jazz({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      AccountSchema={Account}
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

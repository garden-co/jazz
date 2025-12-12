"use client";

import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";
import { apiKey } from "./apiKey";
import { JazzAccount } from "./schema";

export function Jazz({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      AccountSchema={JazzAccount}
      fallback={<div>Loading...</div>}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
      <JazzInspector />
    </JazzReactProvider>
  );
}

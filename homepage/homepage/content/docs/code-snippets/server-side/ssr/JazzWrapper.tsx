"use client";
import { JazzReactProvider } from "jazz-tools/react";
import { JazzFestAccount } from "./schema";

const apiKey = process.env.NEXT_PUBLIC_JAZZ_API_KEY;

export function JazzWrapper({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      AccountSchema={JazzFestAccount}
      enableSSR // [!code ++]
    >
      {children}
    </JazzReactProvider>
  );
}

"use client"; // tells Next.js that this component can't be server-side rendered. If you're not using Next.js, you can remove it.
import { JazzReactProvider } from "jazz-tools/react";
import { JazzFestAccount } from "@/app/schema";

const apiKey = process.env.NEXT_PUBLIC_JAZZ_API_KEY;

export function JazzWrapper({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
      AccountSchema={JazzFestAccount}
    >
      {children}
    </JazzReactProvider>
  );
}

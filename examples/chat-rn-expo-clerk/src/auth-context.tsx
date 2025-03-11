import { useClerk } from "@clerk/clerk-expo";
// FIXME: why isn't the export working?  IDE is fine, Metro doesn't like the non 'dist' import
import { JazzProviderWithClerk } from "jazz-expo/dist/auth/clerk";
import React, { PropsWithChildren } from "react";
import { apiKey } from "./apiKey";

export function JazzAndAuth({ children }: PropsWithChildren) {
  const clerk = useClerk();

  return (
    <JazzProviderWithClerk
      clerk={clerk}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzProviderWithClerk>
  );
}

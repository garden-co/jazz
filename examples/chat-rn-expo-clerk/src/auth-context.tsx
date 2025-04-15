import { useClerk } from "@clerk/clerk-expo";
import { JazzProviderWithClerk } from "jazz-expo/auth/clerk";
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

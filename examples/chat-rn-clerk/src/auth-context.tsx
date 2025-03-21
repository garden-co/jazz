import { useClerk } from "@clerk/clerk-expo";
import { JazzProviderWithClerk } from "jazz-react-native-auth-clerk";
import React, { PropsWithChildren } from "react";
import { apiKey } from "./apiKey";

export function JazzAndAuth({ children }: PropsWithChildren) {
  const clerk = useClerk();

  return (
    <JazzProviderWithClerk
      clerk={clerk}
      storage="sqlite"
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzProviderWithClerk>
  );
}

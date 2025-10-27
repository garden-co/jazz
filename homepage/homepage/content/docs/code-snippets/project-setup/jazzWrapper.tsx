// [!code hide]
const apiKey = "";

import { JazzReactProvider } from "jazz-tools/react";
import { MyAppAccount } from "./schema";

export function JazzWrapper({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      enableSSR // Renders the components tree in the server using an agent
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzReactProvider>
  );
}

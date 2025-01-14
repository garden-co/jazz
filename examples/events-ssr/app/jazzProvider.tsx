"use client";

import { DemoAuthBasicUI, useDemoAuth } from "jazz-react";

import { JazzProvider } from "jazz-react";
import { EventAccount } from "../schema";

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [demoAuth, demoAuthState] = useDemoAuth({});
  return (
    <>
      <JazzProvider
        auth={demoAuth}
        peer="wss://cloud.jazz.tools/?key=you@example.com"
        AccountSchema={EventAccount}
        exposeCredentialsToServer={true}
      >
        {children}
      </JazzProvider>
      <DemoAuthBasicUI state={demoAuthState} appName="Event Announcement App" />
    </>
  );
}

// Register the Account schema so `useAccount` returns our custom `MyAppAccount`
declare module "jazz-react" {
  interface Register {
    Account: EventAccount;
  }
}

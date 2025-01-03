"use client";

import { JazzAccount } from "@/schema";
import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, authState] = useDemoAuth();

  return (
    <>
      <JazzProvider
        auth={auth}
        AccountSchema={JazzAccount}
        // replace `you@example.com` with your email as a temporary API key
        peer="wss://cloud.jazz.tools/?key=you@example.com"
      >
        {children}
      </JazzProvider>
      {authState.state !== "signedIn" && (
        <DemoAuthBasicUI appName="Jazz Book Shelf" state={authState} />
      )}
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: JazzAccount;
  }
}

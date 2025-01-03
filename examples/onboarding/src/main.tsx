import App from "@/App.tsx";
import "@/index.css";
import { HRAccount } from "@/schema.ts";
import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";
import React from "react";
import ReactDOM from "react-dom/client";

const peer =
  (new URL(window.location.href).searchParams.get(
    "peer",
  ) as `ws://${string}`) ??
  "wss://cloud.jazz.tools/?key=onboarding-example-jazz@garden.co";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, authState] = useDemoAuth();
  return (
    <>
      <JazzProvider AccountSchema={HRAccount} auth={auth} peer={peer}>
        {children}
      </JazzProvider>
      {authState.state !== "signedIn" && (
        <DemoAuthBasicUI appName="Jazz Onboarding" state={authState} />
      )}
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: HRAccount;
  }
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <JazzAndAuth>
      <App />
    </JazzAndAuth>
  </React.StrictMode>,
);

import { JazzProvider, PasskeyAuthBasicUI, usePasskeyAuth } from "jazz-react";
import React from "react";
import ReactDOM from "react-dom/client";
import { PasswordManagerAccount } from "./1_schema.ts";
import App from "./5_App.tsx";
import "./index.css";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, state] = usePasskeyAuth({
    appName: "Jazz Password Manager",
  });

  return (
    <>
      <JazzProvider
        AccountSchema={PasswordManagerAccount}
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=password-manager-example-jazz@garden.co"
      >
        {children}
      </JazzProvider>
      <PasskeyAuthBasicUI state={state} />
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: PasswordManagerAccount;
  }
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <JazzAndAuth>
      <App />
    </JazzAndAuth>
  </React.StrictMode>,
);

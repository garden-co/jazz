import { JazzProvider } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { HashRouter } from "react-router-dom";
import App from "./App.tsx";
import "./index.css";
import { apiKey } from "./apiKey.ts";
import { JazzAccount } from "./schema.ts";

// We use this to identify the app in the passkey auth
export const APPLICATION_NAME = "Magic Link Auth Example";

declare module "jazz-react" {
  export interface Register {
    Account: JazzAccount;
  }
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      AccountSchema={JazzAccount}
    >
      <HashRouter>
        <App />
      </HashRouter>
    </JazzProvider>
  </StrictMode>,
);

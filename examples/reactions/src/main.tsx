import { JazzInspector } from "jazz-inspector";
import { JazzProvider, PasskeyAuthBasicUI } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { Account } from "jazz-tools";
import { apiKey } from "./apiKey";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      AccountSchema={Account}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <PasskeyAuthBasicUI appName="Jazz Reactions Example">
        <App />
      </PasskeyAuthBasicUI>
      <JazzInspector />
    </JazzProvider>
  </StrictMode>,
);

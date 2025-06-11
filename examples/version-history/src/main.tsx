import { DemoAuthBasicUI, JazzProvider } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { JazzInspector } from "jazz-inspector";
import { Account } from "jazz-tools";
import { apiKey } from "./apiKey.ts";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      AccountSchema={Account}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <DemoAuthBasicUI appName="Jazz Version History Example">
        <App />
      </DemoAuthBasicUI>
      <JazzInspector position="bottom right" />
    </JazzProvider>
  </StrictMode>,
);

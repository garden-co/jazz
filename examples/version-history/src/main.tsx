import { DemoAuthBasicUI, JazzReactProvider } from "jazz-tools/react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { JazzInspector } from "jazz-tools/inspector";
import { apiKey } from "./apiKey.ts";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzReactProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <DemoAuthBasicUI appName="Jazz Version History Example">
        <App />
      </DemoAuthBasicUI>
      <JazzInspector position="bottom right" />
    </JazzReactProvider>
  </StrictMode>,
);

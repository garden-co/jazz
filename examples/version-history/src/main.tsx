import { DemoAuthBasicUI, JazzProvider } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { JazzInspector } from "jazz-inspector";
import { apiKey } from "./apiKey.ts";

const url = new URL(window.location.href);
const peer = url.searchParams.get("peer") as `wss://${string}` | `wss://${string}` | null;

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      sync={{
        peer: peer ?? `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <DemoAuthBasicUI appName="Jazz Version History Example">
        <App />
      </DemoAuthBasicUI>
      <JazzInspector position="bottom right" />
    </JazzProvider>
  </StrictMode>,
);

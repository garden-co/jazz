import { JazzProvider, PasskeyAuthBasicUI } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { apiKey } from "./apiKey";

const url = new URL(window.location.href);
const peer = url.searchParams.get("peer") as `wss://${string}` | `ws://${string}` | null;

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      sync={{
        peer: peer ?? `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <PasskeyAuthBasicUI appName="Jazz Reactions Example">
        <App />
      </PasskeyAuthBasicUI>
    </JazzProvider>
  </StrictMode>,
);

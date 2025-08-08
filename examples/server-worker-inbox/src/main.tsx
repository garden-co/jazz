import { JazzInspector } from "jazz-tools/inspector";
import { StrictMode } from "react";
import ReactDOM from "react-dom/client";

import "./index.css";
import { JazzReactProvider } from "jazz-tools/react";
import { apiKey } from "@/apiKey.ts";
import { App } from "./app";

const rootElement = document.getElementById("app");
if (rootElement && !rootElement.innerHTML) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(
    <StrictMode>
      <JazzReactProvider
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
      >
        <JazzInspector />
        <App />
      </JazzReactProvider>
    </StrictMode>,
  );
}

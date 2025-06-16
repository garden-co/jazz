import { JazzInspector } from "jazz-inspector";
import { StrictMode } from "react";
import ReactDOM from "react-dom/client";

import "./index.css";
import { apiKey } from "@/apiKey.ts";
import { JazzProvider } from "jazz-react";
import { Account } from "jazz-tools";
import { App } from "./app";

const rootElement = document.getElementById("app");
if (rootElement && !rootElement.innerHTML) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(
    <StrictMode>
      <JazzProvider
        AccountSchema={Account}
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
      >
        <JazzInspector />
        <App />
      </JazzProvider>
    </StrictMode>,
  );
}

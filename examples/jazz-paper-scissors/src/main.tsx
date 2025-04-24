import { JazzInspector } from "jazz-inspector";
import { StrictMode } from "react";
import ReactDOM from "react-dom/client";

import "./index.css";
import { JazzProvider } from "jazz-react";
import { App } from "./app";

const rootElement = document.getElementById("app");
if (rootElement && !rootElement.innerHTML) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(
    <StrictMode>
      <JazzProvider
        sync={{
          peer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co",
        }}
      >
        <JazzInspector />
        <App />
      </JazzProvider>
    </StrictMode>,
  );
}

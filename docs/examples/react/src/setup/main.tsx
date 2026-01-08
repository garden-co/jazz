import React from "react";
import ReactDOM from "react-dom/client";
import type { WasmDatabaseLike } from "@jazz/react";
import { JazzProvider } from "@jazz/react";
import { App } from "../App.js";

//#region main-setup
// Mock WASM database initialization for documentation
declare function initWasmDatabase(): Promise<WasmDatabaseLike>;

async function main() {
  // Initialize the WASM database
  const db = await initWasmDatabase();

  // Render the app with JazzProvider
  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <JazzProvider database={db}>
        <App />
      </JazzProvider>
    </React.StrictMode>
  );
}

main();
//#endregion

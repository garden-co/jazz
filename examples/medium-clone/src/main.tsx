import { installBrowserTelemetry } from "./telemetry.js";
import { createRoot } from "react-dom/client";
import { Suspense } from "react";
import { App } from "./App.js";

installBrowserTelemetry();

createRoot(document.getElementById("root")!).render(
  <Suspense fallback={<div>Loading...</div>}>
    <App />
  </Suspense>,
);

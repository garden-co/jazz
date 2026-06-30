import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createEmbeddedJazzClient } from "jazz-tools/react";
import { InspectorApp } from "./inspector-app";
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <InspectorApp client={createEmbeddedJazzClient()} isOverlay />
  </StrictMode>,
);

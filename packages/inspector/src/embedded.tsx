import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { InspectorApp } from "./inspector-app";
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <InspectorApp />
  </StrictMode>,
);

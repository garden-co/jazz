import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createExtensionJazzClient } from "jazz-tools/react";
import { ExtensionApp } from "./extension-app";
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ExtensionApp client={createExtensionJazzClient()} />
  </StrictMode>,
);

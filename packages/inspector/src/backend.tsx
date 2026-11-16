import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BackendApp } from "./backend/BackendApp";
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BackendApp />
  </StrictMode>,
);

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import "./index.css";
import SessionApp, { readSessionConnection } from "./session/SessionApp.js";
import { completeInspectorCallback } from "./session/browser-session.js";
const callback = completeInspectorCallback();
const connection = callback
  ? null
  : readSessionConnection(import.meta.env.VITE_INSPECTOR_DASHBOARD_ORIGIN);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    {callback ? (
      <p>Completing Inspector login…</p>
    ) : connection ? (
      <SessionApp connection={connection} />
    ) : (
      <App />
    )}
  </StrictMode>,
);

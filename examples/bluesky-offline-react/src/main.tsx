import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import "./styles.css";

const rootElement = document.getElementById("root");

if (!rootElement) {
  throw new Error('Could not find the app root element with id "root"');
}

if ("serviceWorker" in navigator && import.meta.env.PROD) {
  addEventListener("load", () => {
    navigator.serviceWorker.register("/service-worker.js").catch((error: unknown) => {
      console.error("Could not install offline support", error);
    });
  });
}

// JazzProvider owns an OPFS leader worker. StrictMode's development-only
// remount races that worker's lock cleanup and turns a local-cache open into a
// transport-timeout retry, so keep this stateful root single-mounted.
createRoot(rootElement).render(<App />);

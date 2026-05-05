import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import { Suspense } from "react";

createRoot(document.getElementById("root")!).render(
  <Suspense fallback={<div>Loading...</div>}>
    <App />
  </Suspense>,
);

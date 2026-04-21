import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import { Suspense } from "react";
import { Toaster } from "sonner";

createRoot(document.getElementById("root")!).render(
  <Suspense fallback={<div>Loading...</div>}>
    <App />
    <Toaster />
  </Suspense>,
);

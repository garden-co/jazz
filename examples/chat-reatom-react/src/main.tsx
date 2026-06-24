import "./setup";
import "./app.css";
import { createRoot } from "react-dom/client";
import { Toaster } from "@/components/ui/sonner";
import { App } from "./App.js";
import { Suspense } from "react";

createRoot(document.getElementById("root")!).render(
  <>
    <Suspense fallback={<p id="joining-chat">Loading...</p>}>
      <App />
    </Suspense>
    <Toaster />
  </>,
);

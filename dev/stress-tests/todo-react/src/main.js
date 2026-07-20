import { jsx as _jsx } from "react/jsx-runtime";
import { createRoot } from "react-dom/client";
import { App } from "./App.js";
import { Suspense } from "react";
createRoot(document.getElementById("root")).render(
  _jsx(Suspense, { fallback: _jsx("div", { children: "Loading..." }), children: _jsx(App, {}) }),
);

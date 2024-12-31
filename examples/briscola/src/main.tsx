import { RouterProvider } from "@tanstack/react-router";
import { createRoot } from "react-dom/client";
import { JazzAndAuth, useAccount } from "./jazz";
import { router } from "./router";

import "./index.css";

const rootElement = document.getElementById("app")!;

if (!rootElement.innerHTML) {
  const root = createRoot(rootElement);
  root.render(<App />);
}

function App() {
  return (
    <JazzAndAuth>
      <Router />
    </JazzAndAuth>
  );
}

function Router() {
  const { me } = useAccount();

  return <RouterProvider router={router} context={{ me }} />;
}

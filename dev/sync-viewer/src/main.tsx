import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App } from "./App.js";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Telemetry never needs to be retried — if a query fails, surface it.
      retry: false,
      // Default cadence: 3s; per-component overrides as needed.
      refetchInterval: 3000,
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <QueryClientProvider client={queryClient}>
    <App />
  </QueryClientProvider>,
);

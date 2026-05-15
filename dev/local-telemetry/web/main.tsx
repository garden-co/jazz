import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createRoot } from "react-dom/client";
import { App } from "./App.js";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
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

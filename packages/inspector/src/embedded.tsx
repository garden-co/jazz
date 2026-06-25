import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { createEmbeddedJazzClient, JazzClientProvider } from "jazz-tools/react";
import { getRegisteredWasmSchema, onDevToolsPortDisconnect } from "jazz-tools";
import { use, useEffect, useMemo } from "react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import { InspectorRoutes } from "./routes";
import "./index.css";

const client = createEmbeddedJazzClient();

function App() {
  const embeddedClient = use(client);
  const wasmSchema = useMemo(() => getRegisteredWasmSchema(), [embeddedClient]);

  useEffect(() => {
    return onDevToolsPortDisconnect(() => {
      window.location.reload();
    });
  }, []);

  if (!embeddedClient || !wasmSchema) {
    return <p>Waiting for runtime devtools connection...</p>;
  }

  return (
    <JazzClientProvider client={embeddedClient}>
      <DevtoolsProvider wasmSchema={wasmSchema} runtime="extension">
        <MemoryRouter>
          <InspectorRoutes />
        </MemoryRouter>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

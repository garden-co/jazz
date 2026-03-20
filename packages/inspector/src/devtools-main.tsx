import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { createExtensionJazzClient, JazzClientProvider } from "jazz-tools/react";
import { getRegisteredWasmSchema, onDevToolsPortDisconnect } from "jazz-tools";
import { use, useEffect, useMemo } from "react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import { InspectorRoutes } from "./routes";
import "./index.css";

const client = createExtensionJazzClient();

function App() {
  const extensionClient = use(client);
  const wasmSchema = useMemo(() => getRegisteredWasmSchema(), [extensionClient]);

  useEffect(() => {
    return onDevToolsPortDisconnect(() => {
      window.location.reload();
    });
  }, []);

  if (!extensionClient || !wasmSchema) {
    return <p>Waiting for runtime devtools connection...</p>;
  }

  return (
    <JazzClientProvider client={extensionClient}>
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

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createExtensionJazzClient, JazzClientProvider } from "jazz-tools/react";
import { getRegisteredWasmSchema, onDevToolsPortDisconnect } from "jazz-tools";
import { use, useEffect, useMemo } from "react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import { InspectorRouterProvider, createInspectorMemoryRouter } from "./createInspectorRouter";
import "./index.css";

const client = createExtensionJazzClient();
const router = createInspectorMemoryRouter();

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
        <InspectorRouterProvider router={router} />
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

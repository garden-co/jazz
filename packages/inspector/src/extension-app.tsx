import { use, useEffect, useMemo } from "react";
import { MemoryRouter } from "react-router";
import { JazzClientProvider, type JazzClient } from "jazz-tools/react";
import { getRegisteredWasmSchema, onDevToolsPortDisconnect } from "jazz-tools";
import { DevtoolsProvider } from "./contexts/devtools-context";
import { InspectorRoutes } from "./routes";

/**
 * The inspector React tree for the bridge-connected extension devtools panel.
 * The dev-overlay iframe used to share this tree; it now opens its own worker
 * connection instead (see InspectorApp).
 */
export function ExtensionApp({ client }: { client: Promise<JazzClient> }) {
  const resolvedClient = use(client);
  const wasmSchema = useMemo(() => getRegisteredWasmSchema(), [resolvedClient]);

  useEffect(() => {
    return onDevToolsPortDisconnect(() => {
      window.location.reload();
    });
  }, []);

  if (!resolvedClient || !wasmSchema) {
    return <p>Waiting for runtime devtools connection...</p>;
  }

  return (
    <JazzClientProvider client={resolvedClient}>
      <DevtoolsProvider wasmSchema={wasmSchema} runtime="extension">
        <MemoryRouter>
          <InspectorRoutes />
        </MemoryRouter>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

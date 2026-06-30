import { use, useEffect, useMemo, useState } from "react";
import { MemoryRouter } from "react-router";
import { JazzClientProvider, createJazzClient, type JazzClient } from "jazz-tools/react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import {
  readInspectorHostConfig,
  readInspectorHostSchema,
  useHostSubscriptions,
} from "./contexts/host-link";
import { InspectorRoutes } from "./routes";

/**
 * The dev-overlay inspector. Same-origin with the host page, it reads the
 * connection config the loader published on `window.__jazzInspectorHost`, opens
 * its OWN worker connection (like the standalone build), and shows the host's
 * active subscriptions from the one-way push. No devtools bridge.
 */
export function InspectorApp() {
  const config = useMemo(() => readInspectorHostConfig(), []);

  // Schema is plain data injected by the host; it may not be ready until the
  // host has run a query, so poll briefly until it resolves.
  const [wasmSchema, setWasmSchema] = useState(() => readInspectorHostSchema());
  useEffect(() => {
    if (wasmSchema) return;
    const timer = setInterval(() => {
      const next = readInspectorHostSchema();
      if (next) {
        setWasmSchema(next);
        clearInterval(timer);
      }
    }, 250);
    return () => clearInterval(timer);
  }, [wasmSchema]);

  const clientPromise = useMemo<Promise<JazzClient> | null>(() => {
    if (!config) return null;
    return createJazzClient({
      appId: config.appId,
      serverUrl: config.serverUrl,
      env: config.env,
      userBranch: config.userBranch,
      adminSecret: config.adminSecret,
      driver: { type: "memory" },
    });
  }, [config]);

  const hostSubscriptions = useHostSubscriptions();

  // `use` may be called conditionally (unlike other hooks).
  const client = clientPromise ? use(clientPromise) : null;

  if (!config) {
    return <p style={{ padding: 16 }}>Inspector not attached (no host dev plugin detected).</p>;
  }
  if (!client || !wasmSchema) {
    return <p style={{ padding: 16 }}>Connecting…</p>;
  }

  return (
    <JazzClientProvider client={client}>
      <DevtoolsProvider
        wasmSchema={wasmSchema}
        runtime="overlay"
        isOverlay
        hostSubscriptions={hostSubscriptions}
      >
        <MemoryRouter>
          <InspectorRoutes />
        </MemoryRouter>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

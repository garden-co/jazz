import { useEffect, useState } from "react";
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
 * its OWN worker connection (like the standalone build, inheriting the host's
 * credential), and shows the host's active subscriptions from the one-way push.
 * No devtools bridge.
 */
export function InspectorApp() {
  // Config + schema are read from the host handle; poll briefly in case the
  // host attaches a tick after the iframe loads, and the schema isn't ready
  // until the host has created a client.
  const [config, setConfig] = useState(() => readInspectorHostConfig());
  const [wasmSchema, setWasmSchema] = useState(() => readInspectorHostSchema());

  useEffect(() => {
    if (config && wasmSchema) return;
    const timer = setInterval(() => {
      if (!config) {
        const next = readInspectorHostConfig();
        if (next) setConfig(next);
      }
      if (!wasmSchema) {
        const next = readInspectorHostSchema();
        if (next) setWasmSchema(next);
      }
    }, 200);
    return () => clearInterval(timer);
  }, [config, wasmSchema]);

  const [client, setClient] = useState<JazzClient | null>(null);
  const [connectError, setConnectError] = useState<string | null>(null);

  useEffect(() => {
    if (!config) return;
    let cancelled = false;
    let created: JazzClient | null = null;
    setConnectError(null);
    // Pass exactly one credential — secret/jwtToken/cookieSession are mutually
    // exclusive, and a local-first host carries both a secret and a derived
    // jwtToken. Prefer admin (see-everything) → the live session token → seed.
    const credential = config.adminSecret
      ? { adminSecret: config.adminSecret }
      : config.jwtToken
        ? { jwtToken: config.jwtToken }
        : config.secret
          ? { secret: config.secret }
          : {};
    createJazzClient({
      appId: config.appId,
      serverUrl: config.serverUrl,
      env: config.env,
      userBranch: config.userBranch,
      ...credential,
      driver: { type: "memory" },
    })
      .then((c) => {
        if (cancelled) {
          void c.shutdown();
          return;
        }
        created = c;
        setClient(c);
      })
      .catch((error: unknown) => {
        if (!cancelled) setConnectError(error instanceof Error ? error.message : String(error));
      });
    return () => {
      cancelled = true;
      void created?.shutdown();
    };
  }, [config]);

  const hostSubscriptions = useHostSubscriptions();

  if (connectError) {
    return <p style={{ padding: 16 }}>Inspector connection failed: {connectError}</p>;
  }
  if (!config || !wasmSchema || !client) {
    return <p style={{ padding: 16 }}>Connecting…</p>;
  }

  return (
    <JazzClientProvider client={client}>
      <DevtoolsProvider
        wasmSchema={wasmSchema}
        runtime="overlay"
        hostSubscriptions={hostSubscriptions}
      >
        <MemoryRouter>
          <InspectorRoutes />
        </MemoryRouter>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}

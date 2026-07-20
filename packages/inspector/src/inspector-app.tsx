import { Component, useEffect, useRef, useState, type ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { JazzProvider } from "jazz-tools/react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import { readInspectorHostConfig, readInspectorHostSchema } from "./contexts/host-link";
import { InspectorRoutes } from "./routes";

// How long to keep polling for the host handle before giving up and showing an
// error instead of spinning on "Connecting…" forever (e.g. the host never
// mounted the loader, or its schema getter keeps throwing).
const HOST_POLL_INTERVAL_MS = 200;
const HOST_POLL_TIMEOUT_MS = 15_000;

class InspectorConnectionErrorBoundary extends Component<
  { children: ReactNode },
  { error: Error | null }
> {
  constructor(props: { children: ReactNode }) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): { error: Error } {
    return { error };
  }

  render(): ReactNode {
    if (this.state.error) {
      return <p style={{ padding: 16 }}>Inspector connection failed: {this.state.error.message}</p>;
    }
    return this.props.children;
  }
}

/**
 * The dev-overlay inspector. Same-origin with the host page, it reads the
 * connection config the loader published on `window.__jazzInspectorHost`, opens
 * its OWN worker connection (like the standalone build, inheriting the host's
 * credential) via the shared `JazzProvider` — reusing its StrictMode-safe,
 * refcounted client lifecycle rather than hand-rolling one — and shows the
 * host's active subscriptions from the one-way push. No devtools bridge.
 */
export function InspectorApp() {
  // Config + schema are read from the host handle; poll briefly in case the
  // host attaches a tick after the iframe loads, and the schema isn't ready
  // until the host has created a client. Give up after HOST_POLL_TIMEOUT_MS so
  // a host that never appears shows an error instead of polling forever.
  const [config, setConfig] = useState(() => readInspectorHostConfig());
  const [wasmSchema, setWasmSchema] = useState(() => readInspectorHostSchema());
  const [hostTimedOut, setHostTimedOut] = useState(false);
  const pollDeadlineRef = useRef<number>(Date.now() + HOST_POLL_TIMEOUT_MS);

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
      if (Date.now() >= pollDeadlineRef.current) {
        clearInterval(timer);
        setHostTimedOut(true);
      }
    }, HOST_POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [config, wasmSchema]);

  if (!config || !wasmSchema) {
    if (hostTimedOut) {
      return (
        <p style={{ padding: 16 }}>
          Inspector: no host connection found. Is this page running under the Jazz dev plugin?
        </p>
      );
    }
    return <p style={{ padding: 16 }}>Connecting…</p>;
  }

  return (
    <InspectorConnectionErrorBoundary>
      <JazzProvider config={config} fallback={<p style={{ padding: 16 }}>Connecting…</p>}>
        <DevtoolsProvider wasmSchema={wasmSchema} runtime="overlay">
          <MemoryRouter>
            <InspectorRoutes />
          </MemoryRouter>
        </DevtoolsProvider>
      </JazzProvider>
    </InspectorConnectionErrorBoundary>
  );
}

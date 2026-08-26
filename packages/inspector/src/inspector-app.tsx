import { Component, useEffect, useState, type ReactNode } from "react";
import { MemoryRouter } from "react-router";
import type { DbConfig, WasmSchema } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { DevtoolsProvider } from "./contexts/devtools-context";
import {
  openInspectorRuntimeSession,
  readInspectorHostConfig,
  type InspectorRuntimeContext,
  type InspectorRuntimeSession,
} from "./contexts/host-link";
import { InspectorRoutes } from "./routes";

// How long to keep polling for the host handle before giving up and showing an
// error instead of spinning on "Connecting…" forever (e.g. the host never
// mounted the loader, or its schema getter keeps throwing).
const HOST_POLL_INTERVAL_MS = 200;
const HOST_POLL_TIMEOUT_MS = 15_000;

function runtimeContextsEqual(
  left: InspectorRuntimeContext[],
  right: InspectorRuntimeContext[],
): boolean {
  return (
    left.length === right.length &&
    left.every((context, index) => {
      const candidate = right[index];
      return (
        candidate?.key === context.key &&
        candidate.appId === context.appId &&
        candidate.dbName === context.dbName &&
        JSON.stringify(candidate.schema) === JSON.stringify(context.schema)
      );
    })
  );
}

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
 * its own browser client over a peer port minted by the host's SharedWorker.
 * Its main-thread Db remains in-memory while the BrowserConnectionManager
 * joins the selected worker-owned context. The provider
 * supplies the StrictMode-safe,
 * refcounted client lifecycle rather than hand-rolling one — and shows the
 * host's active subscriptions from the one-way push. No devtools bridge.
 */
export function InspectorApp() {
  const [session, setSession] = useState<InspectorRuntimeSession | null>(null);
  const [contexts, setContexts] = useState<InspectorRuntimeContext[]>([]);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [connection, setConnection] = useState<{
    config: DbConfig;
    schema: WasmSchema;
  } | null>(null);
  const [hostTimedOut, setHostTimedOut] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;
    let activeSession: InspectorRuntimeSession | null = null;
    const deadline = Date.now() + HOST_POLL_TIMEOUT_MS;
    const connect = async () => {
      while (active && Date.now() < deadline) {
        try {
          const next = await openInspectorRuntimeSession();
          if (next && next.contexts.length > 0) {
            if (!active) {
              next.close();
              return;
            }
            activeSession = next;
            setSession(next);
            setContexts(next.contexts);
            setSelectedKey(next.contexts[0]!.key);
            return;
          }
          next?.close();
        } catch {
          // The host runtime may still be starting. Retry until the deadline.
        }
        await new Promise((resolve) => setTimeout(resolve, HOST_POLL_INTERVAL_MS));
      }
      if (active) setHostTimedOut(true);
    };
    void connect();
    return () => {
      active = false;
      activeSession?.close();
    };
  }, []);

  useEffect(() => {
    if (!session) return;
    const timer = setInterval(() => {
      void session.listContexts().then((next) => {
        setContexts((current) => (runtimeContextsEqual(current, next) ? current : next));
        setSelectedKey((current) =>
          current && next.some((context) => context.key === current)
            ? current
            : (next[0]?.key ?? null),
        );
      });
    }, 1_000);
    return () => clearInterval(timer);
  }, [session]);

  useEffect(() => {
    if (!session || !selectedKey) return;
    const context = contexts.find((candidate) => candidate.key === selectedKey);
    const hostConfig = readInspectorHostConfig();
    if (!context || !hostConfig) return;
    let active = true;
    setConnection(null);
    setError(null);
    void session
      .attach(selectedKey)
      .then((browserWorkerPort) => {
        if (!active) {
          browserWorkerPort.close();
          return;
        }
        setConnection({
          config: {
            ...hostConfig,
            appId: context.appId,
            driver: { type: "persistent", dbName: context.dbName },
            runtimeSources: { browserWorkerPort },
          },
          schema: context.schema,
        });
      })
      .catch((cause: unknown) => {
        if (active) setError(cause instanceof Error ? cause : new Error(String(cause)));
      });
    return () => {
      active = false;
    };
  }, [contexts, selectedKey, session]);

  if (error) return <p style={{ padding: 16 }}>Inspector connection failed: {error.message}</p>;
  if (!connection) {
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
    <>
      {contexts.length > 1 ? (
        <label style={{ display: "block", padding: "8px 12px" }}>
          Runtime context{" "}
          <select
            value={selectedKey ?? ""}
            onChange={(event) => setSelectedKey(event.target.value)}
          >
            {contexts.map((context) => (
              <option key={context.key} value={context.key}>
                {context.appId} / {context.dbName}
              </option>
            ))}
          </select>
        </label>
      ) : null}
      <InspectorRuntime
        key={selectedKey}
        config={connection.config}
        wasmSchema={connection.schema}
      />
    </>
  );
}

function InspectorRuntime({ config, wasmSchema }: { config: DbConfig; wasmSchema: WasmSchema }) {
  const initialRoute = new URLSearchParams(window.location.search).get("route") ?? "/";
  return (
    <InspectorConnectionErrorBoundary>
      <JazzProvider
        config={config}
        autoAttachDevTools={false}
        fallback={<p style={{ padding: 16 }}>Connecting…</p>}
      >
        <DevtoolsProvider wasmSchema={wasmSchema} runtime="overlay">
          <MemoryRouter initialEntries={[initialRoute]}>
            <InspectorRoutes />
          </MemoryRouter>
        </DevtoolsProvider>
      </JazzProvider>
    </InspectorConnectionErrorBoundary>
  );
}

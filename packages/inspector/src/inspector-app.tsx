import { Component, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { JazzProvider } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { DevtoolsProvider } from "./contexts/devtools-context";
import {
  readInspectorHostConfig,
  readInspectorHostSchema,
  useHostSubscriptions,
} from "./contexts/host-link";
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

  const hostSubscriptions = useHostSubscriptions();

  // Pass exactly one identity credential — secret/jwtToken/cookieSession are
  // mutually exclusive, and a local-first host carries both a secret and a
  // derived jwtToken. Use the host's *identity* (live session → seed) so the
  // overlay is the same user as the host and reads its local store. adminSecret
  // is independent of identity (not mutually exclusive with it) and, when
  // present, always wins the broker's authClass fingerprint — see
  // resolveBrokerAuthClass — so it must always be forwarded when the host has
  // one, regardless of which identity credential is also set.
  //
  // Memoized on `config` (which, once set, is a stable reference — the poll
  // effect above stops re-setting it as soon as both config and wasmSchema are
  // present): otherwise every render — including one triggered by every host
  // subscription push — would rebuild this object graph and re-run
  // JazzProvider's JSON.stringify(config) client-registry key for no reason.
  const dbConfig: DbConfig | null = useMemo(() => {
    if (!config) return null;
    const identityCredential = config.jwtToken
      ? { jwtToken: config.jwtToken }
      : config.secret
        ? { secret: config.secret }
        : config.cookieSession
          ? { cookieSession: config.cookieSession }
          : {};
    const credential = {
      ...identityCredential,
      ...(config.adminSecret ? { adminSecret: config.adminSecret } : {}),
    };
    return {
      appId: config.appId,
      serverUrl: config.serverUrl,
      env: config.env,
      userBranch: config.userBranch,
      ...credential,
      // Join the host's persistent store: the host's resolved OPFS namespace
      // (dbName) and exact broker SharedWorker URL, so the overlay sees the
      // host's local data — including unsynced local-only rows — and works
      // offline.
      driver: { type: "persistent", dbName: config.dbName },
      runtimeSources: { brokerWorkerUrl: config.brokerWorkerUrl },
    };
  }, [config]);

  if (!dbConfig || !wasmSchema) {
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
      <JazzProvider
        config={dbConfig}
        autoAttachDevTools={false}
        fallback={<p style={{ padding: 16 }}>Connecting…</p>}
      >
        <DevtoolsProvider
          wasmSchema={wasmSchema}
          runtime="overlay"
          hostSubscriptions={hostSubscriptions}
        >
          <MemoryRouter>
            <InspectorRoutes />
          </MemoryRouter>
        </DevtoolsProvider>
      </JazzProvider>
    </InspectorConnectionErrorBoundary>
  );
}

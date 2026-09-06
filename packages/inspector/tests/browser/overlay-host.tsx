// Browser-test fixture for the NEW overlay model: a real host app connects to
// the test sync server, publishes the `window.__jazzInspectorHost` handle (the
// same shape the loader's installInspectorHost builds), pushes its active
// subscription list to the embedded inspector iframe, and the overlay opens an
// independent browser client that joins a selected context through a peer port
// minted by the host's SharedWorker. No devtools bridge.
//
// Exercised by overlay.spec.ts.
import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider, useAll, useJazzClient } from "jazz-tools/react";
import {
  createAccountManager,
  installInspectorHost,
  type DbConfig,
  type AccountHandle,
} from "jazz-tools";
import { app } from "./schema.js";

// Mirrors tests/browser/test-constants.ts (inlined: that module reads process.env).
const APP_ID = "00000000-0000-0000-0000-000000000099";
const TEST_ENV = "dev";
const TEST_PORT = 19879;
const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;

function HostInner({ secondaryReady }: { secondaryReady: boolean }) {
  const { db } = useJazzClient();
  // A real query: creates the host client (so getRuntimeSchema resolves) and
  // registers a public subscription the overlay's Subscriptions tab should display.
  const { data: readinessTodos = [] } = useAll(
    app.todos.where({ title: { in: ["First seeded todo", "Second seeded todo"] } }).limit(2),
    { tier: "edge" },
  );
  const primaryReady = readinessTodos.some((todo) => todo.title === "First seeded todo");
  const iframeRef = useRef<HTMLIFrameElement>(null);

  useEffect(() => {
    if (!secondaryReady || !primaryReady) return;
    const iframeWindow = iframeRef.current?.contentWindow;
    if (!iframeWindow) return;
    // The real host-side installer: publishes the handle + pushes subscriptions.
    return installInspectorHost(db, iframeWindow, window.location.origin);
  }, [db, primaryReady, secondaryReady]);

  if (!secondaryReady) return <p id="host-status">Starting secondary runtime...</p>;
  if (!primaryReady) return <p id="host-status">Loading primary runtime data...</p>;

  return (
    <>
      <p id="host-status">Host ready</p>
      <iframe
        ref={iframeRef}
        title="jazz-inspector"
        // overlay.spec.ts serves dist-embedded/ at this path via a Playwright route.
        src="/__jazz/embedded/embedded.html"
        style={{ width: 900, height: 640, border: "1px solid #ccc" }}
      />
    </>
  );
}

function SecondaryRuntime({ onReady }: { onReady: () => void }) {
  useAll(app.todos);
  useEffect(onReady, [onReady]);
  return <p hidden>Secondary runtime ready</p>;
}

function HostApp({ primary, secondary }: { primary: AccountHandle; secondary: AccountHandle }) {
  const [secondaryReady, setSecondaryReady] = useState(false);

  const config: DbConfig = {
    appId: APP_ID,
    env: TEST_ENV,
    serverUrl: SERVER_URL,
    account: primary,
    // devMode must be on at subscribe time for subscription traces to register.
    // Under the jazz dev plugin the provider defaults it on automatically, but
    // this fixture runs under the inspector's own vite server (no plugin flag),
    // so set it explicitly like the provider would.
    devMode: true,
  };

  return (
    <>
      <JazzProvider
        config={config}
        autoAttachDevTools={false}
        fallback={<p id="host-status">Connecting...</p>}
      >
        <HostInner secondaryReady={secondaryReady} />
      </JazzProvider>
      <JazzProvider
        config={{
          appId: APP_ID,
          env: TEST_ENV,
          account: secondary,
          // Share the logical base: account IDs must isolate the physical roots.
          driver: { type: "persistent", dbName: APP_ID },
          devMode: true,
        }}
        autoAttachDevTools={false}
      >
        <SecondaryRuntime onReady={() => setSecondaryReady(true)} />
      </JazzProvider>
    </>
  );
}

const accounts = await createAccountManager({ appId: APP_ID, serverUrl: SERVER_URL });
const primary = accounts.createLocalFirst();
const secondary = accounts.createLocalFirst();
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HostApp primary={primary} secondary={secondary} />
  </StrictMode>,
);

// Browser-test fixture for the NEW overlay model: a real host app connects to
// the test sync server, publishes the `window.__jazzInspectorHost` handle (the
// same shape the loader's installInspectorHost builds), pushes its active
// subscription list to the embedded inspector iframe, and the overlay opens its
// OWN worker connection from the published config. No devtools bridge.
//
// Exercised by overlay.spec.ts.
import { StrictMode, useEffect, useRef } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider, useAll, useJazzClient, useLocalFirstAuth } from "jazz-tools/react";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type DbConfig,
} from "jazz-tools";
import { app } from "./schema.js";

// Mirrors tests/browser/test-constants.ts (inlined: that module reads process.env).
const APP_ID = "00000000-0000-0000-0000-000000000099";
const TEST_ENV = "dev";
const TEST_BRANCH = "main";
const TEST_PORT = 19879;
const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;

function HostInner() {
  const { db } = useJazzClient();
  // A real query: creates the host client (so getRuntimeSchema resolves) and
  // registers a public subscription the overlay's Live Query should display.
  useAll(app.todos);
  const iframeRef = useRef<HTMLIFrameElement>(null);

  useEffect(() => {
    const iframeWindow = iframeRef.current?.contentWindow;
    if (!iframeWindow) return;

    db.setDevMode(true);
    (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = {
      getConnectionConfig() {
        const c = db.getConfig();
        return {
          appId: c.appId,
          serverUrl: c.serverUrl,
          env: c.env ?? "",
          userBranch: c.userBranch,
          secret: c.secret,
          adminSecret: c.adminSecret,
          jwtToken: c.jwtToken,
        };
      },
      getWasmSchema: () => db.getRuntimeSchema(),
      getActiveSubscriptions: () => serializeActiveSubscriptions(db.getActiveQuerySubscriptions()),
    };

    const push = () =>
      iframeWindow.postMessage(
        {
          type: INSPECTOR_SUBSCRIPTIONS_MESSAGE,
          list: serializeActiveSubscriptions(db.getActiveQuerySubscriptions()),
        },
        window.location.origin,
      );
    const stop = db.onActiveQuerySubscriptionsChange(push);

    return () => {
      stop();
      delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
    };
  }, [db]);

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

function HostApp() {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) {
    return <p id="host-status">Authenticating...</p>;
  }

  const config: DbConfig = {
    appId: APP_ID,
    env: TEST_ENV,
    userBranch: TEST_BRANCH,
    serverUrl: SERVER_URL,
    secret,
  };

  return (
    <JazzProvider
      config={config}
      autoAttachDevTools={false}
      fallback={<p id="host-status">Connecting...</p>}
    >
      <HostInner />
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HostApp />
  </StrictMode>,
);

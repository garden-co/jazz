// Browser-test fixture: a real host app that mirrors what a consumer's page does
// when running the Jazz inspector overlay. It builds a real Jazz client + schema
// via the public API, calls `attachDevTools(client, wasmSchema)` (the runtime side
// of the `jazz-devtools-v1` bridge), embeds the inspector's EMBEDDED build in an
// iframe, and relays the parent-window postMessage transport between the two.
//
// Exercised by overlay.spec.ts.
import { StrictMode, useEffect, useRef } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider, attachDevTools, useJazzClient, useLocalFirstAuth } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "./schema.js";

// Mirrors tests/browser/test-constants.ts. Inlined rather than imported because
// that module reads `process.env` at top level, which is undefined in the browser.
const APP_ID = "00000000-0000-0000-0000-000000000099";
const TEST_ENV = "dev";
const TEST_BRANCH = "main";
const TEST_PORT = 19879;

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;

// Inlined copy of `createRelay` from
// packages/jazz-tools/src/dev/inspector-overlay/relay.ts. The relay is not
// exported from a public entry point, so the host page carries its own minimal
// copy. Keep in sync with relay.ts: it re-injects bridge *requests* coming from
// the iframe into the top window (so `attachDevTools`'s `event.source === window`
// guard accepts them) and forwards *responses*/*events* from the top window back
// into the iframe.
const DEVTOOLS_BRIDGE_CHANNEL = "jazz-devtools-v1";

function isBridgeMessage(data: unknown): data is { channel: string; kind: string } {
  return (
    typeof data === "object" &&
    data !== null &&
    (data as { channel?: unknown }).channel === DEVTOOLS_BRIDGE_CHANNEL &&
    typeof (data as { kind?: unknown }).kind === "string"
  );
}

function createRelay({
  topWindow,
  iframeWindow,
  origin,
}: {
  topWindow: Window;
  iframeWindow: Window;
  origin: string;
}) {
  function handle(event: MessageEvent): void {
    const data = event.data;
    if (!isBridgeMessage(data)) return;
    if (event.source === iframeWindow) {
      if (data.kind !== "request") return;
      topWindow.postMessage(data, "*");
      return;
    }
    if (event.source === topWindow) {
      if (data.kind !== "response" && data.kind !== "event") return;
      iframeWindow.postMessage(data, origin);
      return;
    }
  }
  return { handle };
}

function OverlayBridge() {
  const { db } = useJazzClient();
  const iframeRef = useRef<HTMLIFrameElement>(null);

  // Runtime side of the bridge: announce this client + schema over postMessage.
  useEffect(() => {
    void attachDevTools({ db }, app.wasmSchema);
  }, [db]);

  // Wire the relay between the top window and the embedded inspector iframe.
  useEffect(() => {
    const iframe = iframeRef.current;
    const iframeWindow = iframe?.contentWindow;
    if (!iframeWindow) return;
    const relay = createRelay({
      topWindow: window,
      iframeWindow,
      origin: window.location.origin,
    });
    const onMessage = (event: MessageEvent) => relay.handle(event);
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, []);

  return (
    <iframe
      ref={iframeRef}
      title="jazz-inspector"
      // The embedded Vite build emits embedded.html; overlay.spec.ts serves
      // dist-embedded/ at this path via a Playwright route.
      src="/__jazz/embedded/embedded.html"
      style={{ width: 720, height: 640, border: "1px solid #ccc" }}
    />
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
    // Attach devtools manually below (autoAttachDevTools={false}) so the test
    // exercises the explicit `attachDevTools(client, wasmSchema)` path.
    <JazzProvider
      config={config}
      autoAttachDevTools={false}
      fallback={<p id="host-status">Connecting...</p>}
    >
      <p id="host-status">Host ready</p>
      <OverlayBridge />
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HostApp />
  </StrictMode>,
);

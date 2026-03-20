const BRIDGE_CHANNEL = "jazz-devtools-v1";
const PORT_NAME = "jazz-inspector-devtools";
const DEVTOOLS_CONNECTED_EVENT = "devtools.connected";
const DEVTOOLS_DISCONNECTED_EVENT = "devtools.disconnected";
const COMLINK_CONNECT_KIND = "devtools.comlink.connect";
const COMLINK_READY_KIND = "devtools.comlink.ready";
const PAGE_BRIDGE_CONNECT_TIMEOUT_MS = 2000;
const PAGE_BRIDGE_RETRY_INTERVAL_MS = 500;

chrome.runtime.onConnect.addListener((port) => {
  if (port.name !== PORT_NAME) return;

  let disposed = false;
  let pagePort = null;
  let connectPromise = null;

  const waitForRetry = (ms) =>
    new Promise((resolve) => {
      setTimeout(resolve, ms);
    });

  window.postMessage(
    {
      channel: BRIDGE_CHANNEL,
      kind: "event",
      event: DEVTOOLS_CONNECTED_EVENT,
    },
    "*",
  );

  const onPagePortMessage = (event) => {
    port.postMessage(event.data);
  };

  const detachPagePort = () => {
    if (!pagePort) return;
    pagePort.removeEventListener("message", onPagePortMessage);
    pagePort.close();
    pagePort = null;
  };

  const ensurePagePort = async () => {
    if (pagePort) return pagePort;
    if (connectPromise) return connectPromise;

    connectPromise = (async () => {
      while (!disposed) {
        const channel = new MessageChannel();
        const relayPort = channel.port1;
        const exposedPort = channel.port2;

        const connected = await new Promise((resolve) => {
          let settled = false;

          const cleanup = () => {
            relayPort.removeEventListener("message", onReadyMessage);
            clearTimeout(timeoutId);
          };

          const onReadyMessage = (event) => {
            if (settled) return;
            const data = event.data;
            if (!data || typeof data !== "object") return;
            if (data.channel !== BRIDGE_CHANNEL || data.kind !== COMLINK_READY_KIND) return;
            settled = true;
            cleanup();
            resolve(true);
          };

          const timeoutId = setTimeout(() => {
            if (settled) return;
            settled = true;
            cleanup();
            relayPort.close();
            resolve(false);
          }, PAGE_BRIDGE_CONNECT_TIMEOUT_MS);

          relayPort.addEventListener("message", onReadyMessage);
          relayPort.start?.();
          window.postMessage(
            {
              channel: BRIDGE_CHANNEL,
              kind: COMLINK_CONNECT_KIND,
            },
            "*",
            [exposedPort],
          );
        });

        if (connected) {
          pagePort = relayPort;
          pagePort.addEventListener("message", onPagePortMessage);
          pagePort.start?.();
          return pagePort;
        }

        await waitForRetry(PAGE_BRIDGE_RETRY_INTERVAL_MS);
      }

      throw new Error("DevTools page bridge unavailable.");
    })().finally(() => {
      connectPromise = null;
    });

    return connectPromise;
  };

  const onWindowMessage = (event) => {
    if (event.source !== window) return;
    const data = event.data;
    if (!data || typeof data !== "object") return;
    if (data.channel !== BRIDGE_CHANNEL) return;
    if (data.kind !== "event") return;
    port.postMessage(data);
  };

  const onPortMessage = (message) => {
    void ensurePagePort()
      .then((connectedPagePort) => {
        connectedPagePort.postMessage(message);
      })
      .catch(() => undefined);
  };

  const dispose = () => {
    disposed = true;
    detachPagePort();
    window.postMessage(
      {
        channel: BRIDGE_CHANNEL,
        kind: "event",
        event: DEVTOOLS_DISCONNECTED_EVENT,
      },
      "*",
    );
    window.removeEventListener("message", onWindowMessage);
    port.onMessage.removeListener(onPortMessage);
    port.onDisconnect.removeListener(dispose);
  };

  window.addEventListener("message", onWindowMessage);
  port.onMessage.addListener(onPortMessage);
  port.onDisconnect.addListener(dispose);
  void ensurePagePort().catch(() => undefined);
});

const BRIDGE_CHANNEL = "jazz-devtools-v1";
const PORT_NAME = "jazz-inspector-devtools";
const DEVTOOLS_CONNECTED_EVENT = "devtools.connected";
const DEVTOOLS_DISCONNECTED_EVENT = "devtools.disconnected";

chrome.runtime.onConnect.addListener((port) => {
  if (port.name !== PORT_NAME) return;

  window.postMessage(
    {
      channel: BRIDGE_CHANNEL,
      kind: "event",
      event: DEVTOOLS_CONNECTED_EVENT,
    },
    "*",
  );

  const onWindowMessage = (event) => {
    if (event.source !== window) return;
    const data = event.data;
    if (!data || typeof data !== "object") return;
    if (data.channel !== BRIDGE_CHANNEL) return;
    if (data.kind !== "response" && data.kind !== "event") return;
    port.postMessage(data);
  };

  const onPortMessage = (message) => {
    if (!message || typeof message !== "object") return;
    if (message.channel !== BRIDGE_CHANNEL) return;
    if (message.kind !== "request") return;
    window.postMessage(message, "*");
  };

  const dispose = () => {
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
});

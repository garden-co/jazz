import type { DevtoolsBridgePort } from "./extension-panel.js";
import { DEVTOOLS_BRIDGE_CHANNEL, isRecord } from "./protocol.js";

export function createParentWindowBridgePort(): Promise<DevtoolsBridgePort> {
  const messageListeners = new Set<(message: unknown) => void>();
  const disconnectListeners = new Set<() => void>();
  const onWindowMessage = (event: MessageEvent) => {
    if (event.source !== window.parent) return;
    const data = event.data;
    if (!isRecord(data) || data.channel !== DEVTOOLS_BRIDGE_CHANNEL) return;
    for (const listener of messageListeners) listener(data);
  };
  window.addEventListener("message", onWindowMessage);
  const port: DevtoolsBridgePort = {
    postMessage(message: unknown) {
      window.parent.postMessage(message, window.location.origin);
    },
    onMessage: {
      addListener: (cb) => messageListeners.add(cb),
      removeListener: (cb) => messageListeners.delete(cb),
    },
    onDisconnect: {
      addListener: (cb) => disconnectListeners.add(cb),
      removeListener: (cb) => disconnectListeners.delete(cb),
    },
  };
  return Promise.resolve(port);
}

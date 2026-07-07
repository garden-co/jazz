import { DEVTOOLS_BRIDGE_CHANNEL } from "../../dev-tools/protocol.js";

export interface RelayOptions {
  topWindow: Window;
  iframeWindow: Window;
  origin: string;
}

function isBridgeMessage(data: unknown): data is { channel: string; kind: string } {
  return (
    typeof data === "object" &&
    data !== null &&
    (data as { channel?: unknown }).channel === DEVTOOLS_BRIDGE_CHANNEL &&
    typeof (data as { kind?: unknown }).kind === "string"
  );
}

export function createRelay({ topWindow, iframeWindow, origin }: RelayOptions) {
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

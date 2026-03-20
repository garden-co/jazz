import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_CONTROL_MESSAGES,
  DEVTOOLS_EVENTS,
  DEVTOOLS_PORT_NAME,
} from "./protocol.js";

export interface DevtoolsPortRelayConfig {
  bridgeChannel: string;
  portName: string;
  connectedEvent: string;
  disconnectedEvent: string;
  comlinkConnectKind: string;
  comlinkReadyKind: string;
  connectTimeoutMs: number;
  retryIntervalMs: number;
  installMarker: string;
}

export function createDevtoolsPortRelayConfig(
  overrides: Partial<DevtoolsPortRelayConfig> = {},
): DevtoolsPortRelayConfig {
  return {
    bridgeChannel: DEVTOOLS_BRIDGE_CHANNEL,
    portName: DEVTOOLS_PORT_NAME,
    connectedEvent: DEVTOOLS_EVENTS.CONNECTED,
    disconnectedEvent: DEVTOOLS_EVENTS.DISCONNECTED,
    comlinkConnectKind: DEVTOOLS_CONTROL_MESSAGES.COMLINK_CONNECT,
    comlinkReadyKind: DEVTOOLS_CONTROL_MESSAGES.COMLINK_READY,
    connectTimeoutMs: 2_000,
    retryIntervalMs: 500,
    installMarker: "__jazzDevtoolsBridgeInstalledV1",
    ...overrides,
  };
}

type RelayPortListener = (message: unknown) => void;

type RelayPortLike = {
  name: string;
  postMessage(message: unknown): void;
  onMessage: {
    addListener(listener: RelayPortListener): void;
    removeListener(listener: RelayPortListener): void;
  };
  onDisconnect: {
    addListener(listener: () => void): void;
    removeListener(listener: () => void): void;
  };
};

type ChromeRuntimeLike = {
  onConnect: {
    addListener(listener: (port: RelayPortLike) => void): void;
  };
};

export function installDevtoolsPortRelay(config: DevtoolsPortRelayConfig): void {
  const globalWindow = window as unknown as Record<string, unknown>;
  const globalAny = globalThis as { chrome?: { runtime?: ChromeRuntimeLike } };
  const chromeApi = globalAny.chrome;

  if (!chromeApi?.runtime || typeof chromeApi.runtime.onConnect?.addListener !== "function") {
    return;
  }

  if (globalWindow[config.installMarker]) {
    return;
  }
  globalWindow[config.installMarker] = true;

  chromeApi.runtime.onConnect.addListener((port) => {
    if (port.name !== config.portName) {
      return;
    }

    let disposed = false;
    let pagePort: MessagePort | null = null;
    let connectPromise: Promise<MessagePort> | null = null;

    const waitForRetry = (ms: number) =>
      new Promise<void>((resolve) => {
        setTimeout(resolve, ms);
      });

    window.postMessage(
      {
        channel: config.bridgeChannel,
        kind: "event",
        event: config.connectedEvent,
      },
      "*",
    );

    const onPagePortMessage = (event: MessageEvent) => {
      port.postMessage(event.data);
    };

    const detachPagePort = () => {
      if (!pagePort) {
        return;
      }

      pagePort.removeEventListener("message", onPagePortMessage);
      pagePort.close();
      pagePort = null;
    };

    const ensurePagePort = async (): Promise<MessagePort> => {
      if (pagePort) {
        return pagePort;
      }

      if (connectPromise) {
        return connectPromise;
      }

      connectPromise = (async () => {
        while (!disposed) {
          const channel = new MessageChannel();
          const relayPort = channel.port1;
          const exposedPort = channel.port2;

          const connected = await new Promise<boolean>((resolve) => {
            let settled = false;

            const cleanup = () => {
              relayPort.removeEventListener("message", onReadyMessage);
              clearTimeout(timeoutId);
            };

            const onReadyMessage = (event: MessageEvent) => {
              if (settled) return;
              const data = event.data as Record<string, unknown> | null;
              if (!data || typeof data !== "object") return;
              if (data.channel !== config.bridgeChannel || data.kind !== config.comlinkReadyKind) {
                return;
              }
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
            }, config.connectTimeoutMs);

            relayPort.addEventListener("message", onReadyMessage);
            relayPort.start?.();
            window.postMessage(
              {
                channel: config.bridgeChannel,
                kind: config.comlinkConnectKind,
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

          await waitForRetry(config.retryIntervalMs);
        }

        throw new Error("DevTools page bridge unavailable.");
      })().finally(() => {
        connectPromise = null;
      });

      return connectPromise;
    };

    const onWindowMessage = (event: MessageEvent) => {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || typeof data !== "object") return;
      const envelope = data as Record<string, unknown>;
      if (envelope.channel !== config.bridgeChannel) return;
      if (envelope.kind !== "event") return;
      port.postMessage(data);
    };

    const onPortMessage = (message: unknown) => {
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
          channel: config.bridgeChannel,
          kind: "event",
          event: config.disconnectedEvent,
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
}

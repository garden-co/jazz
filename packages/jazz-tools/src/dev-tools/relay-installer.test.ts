import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createDevtoolsPortRelayConfig,
  installDevtoolsPortRelay,
  type DevtoolsPortRelayConfig,
} from "./relay-installer.js";

type WindowMessageListener = (event: {
  source: FakeWindow;
  data: unknown;
  ports: readonly MessagePort[];
}) => void;

class FakeWindow {
  private readonly listeners = new Set<WindowMessageListener>();

  addEventListener(type: string, listener: WindowMessageListener): void {
    if (type === "message") {
      this.listeners.add(listener);
    }
  }

  removeEventListener(type: string, listener: WindowMessageListener): void {
    if (type === "message") {
      this.listeners.delete(listener);
    }
  }

  postMessage(data: unknown, _targetOrigin?: string, transfer?: Transferable[]): void {
    const ports = (transfer ?? []) as MessagePort[];
    for (const listener of Array.from(this.listeners)) {
      listener({ source: this, data, ports });
    }
  }
}

class ListenerStore<TArgs extends unknown[]> {
  private readonly listeners = new Set<(...listenerArgs: TArgs) => void>();

  addListener(listener: (...listenerArgs: TArgs) => void): void {
    this.listeners.add(listener);
  }

  removeListener(listener: (...listenerArgs: TArgs) => void): void {
    this.listeners.delete(listener);
  }

  emit(...args: TArgs): void {
    for (const listener of Array.from(this.listeners)) {
      listener(...args);
    }
  }

  size(): number {
    return this.listeners.size;
  }
}

class FakeRuntimePort {
  readonly onMessage = new ListenerStore<[message: unknown]>();
  readonly onDisconnect = new ListenerStore<[]>();
  readonly forwardedMessages: unknown[] = [];

  constructor(readonly name: string) {}

  postMessage(message: unknown): void {
    this.forwardedMessages.push(message);
  }

  dispatchMessage(message: unknown): void {
    this.onMessage.emit(message);
  }

  disconnect(): void {
    this.onDisconnect.emit();
  }
}

class FakeChromeRuntime {
  private readonly connectListeners = new ListenerStore<[port: FakeRuntimePort]>();

  readonly onConnect = {
    addListener: (listener: (port: FakeRuntimePort) => void) => {
      this.connectListeners.addListener(listener);
    },
  };

  connect(port: FakeRuntimePort): void {
    this.connectListeners.emit(port);
  }

  listenerCount(): number {
    return this.connectListeners.size();
  }
}

const originalWindow = (globalThis as { window?: unknown }).window;
const originalChrome = (globalThis as { chrome?: unknown }).chrome;

afterEach(() => {
  vi.restoreAllMocks();
  if (originalWindow === undefined) {
    delete (globalThis as { window?: unknown }).window;
  } else {
    (globalThis as { window?: unknown }).window = originalWindow;
  }

  if (originalChrome === undefined) {
    delete (globalThis as { chrome?: unknown }).chrome;
  } else {
    (globalThis as { chrome?: unknown }).chrome = originalChrome;
  }
});

function waitForWindowMessage(
  fakeWindow: FakeWindow,
  predicate: (data: unknown) => boolean,
): Promise<{ data: unknown; ports: readonly MessagePort[] }> {
  return new Promise((resolve) => {
    const listener: WindowMessageListener = (event) => {
      if (!predicate(event.data)) {
        return;
      }
      fakeWindow.removeEventListener("message", listener);
      resolve({ data: event.data, ports: event.ports });
    };
    fakeWindow.addEventListener("message", listener);
  });
}

function waitForPortMessage(port: MessagePort): Promise<unknown> {
  return new Promise((resolve) => {
    const listener = (event: MessageEvent) => {
      port.removeEventListener("message", listener);
      resolve(event.data);
    };
    port.addEventListener("message", listener);
  });
}

function waitForRuntimeForward(
  port: FakeRuntimePort,
  predicate: (message: unknown) => boolean,
): Promise<unknown> {
  return new Promise((resolve) => {
    const checkMessages = () => {
      const match = port.forwardedMessages.find(predicate);
      if (match !== undefined) {
        resolve(match);
        return true;
      }
      return false;
    };

    if (checkMessages()) {
      return;
    }

    const intervalId = setInterval(() => {
      if (checkMessages()) {
        clearInterval(intervalId);
      }
    }, 1);
  });
}

function isEventMessage(
  config: DevtoolsPortRelayConfig,
  eventName: string,
): (data: unknown) => boolean {
  return (data) =>
    Boolean(
      data &&
      typeof data === "object" &&
      (data as { channel?: unknown }).channel === config.bridgeChannel &&
      (data as { kind?: unknown }).kind === "event" &&
      (data as { event?: unknown }).event === eventName,
    );
}

describe("installDevtoolsPortRelay", () => {
  it("forwards traffic across the shared page relay port", async () => {
    const fakeWindow = new FakeWindow();
    const fakeRuntime = new FakeChromeRuntime();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;
    (globalThis as { chrome?: unknown }).chrome = {
      runtime: fakeRuntime,
    };

    const config = createDevtoolsPortRelayConfig({
      retryIntervalMs: 1,
    });

    installDevtoolsPortRelay(config);

    const devtoolsPort = new FakeRuntimePort(config.portName);
    const connectedEventPromise = waitForWindowMessage(
      fakeWindow,
      isEventMessage(config, config.connectedEvent),
    );
    const connectMessagePromise = waitForWindowMessage(fakeWindow, (data) =>
      Boolean(
        data &&
        typeof data === "object" &&
        (data as { channel?: unknown }).channel === config.bridgeChannel &&
        (data as { kind?: unknown }).kind === config.comlinkConnectKind,
      ),
    );

    fakeRuntime.connect(devtoolsPort);

    await connectedEventPromise;

    const connectMessage = await connectMessagePromise;
    const pagePort = connectMessage.ports[0];
    expect(pagePort).toBeDefined();

    pagePort!.start?.();
    pagePort!.postMessage({
      channel: config.bridgeChannel,
      kind: config.comlinkReadyKind,
    });

    const pageMessagePromise = waitForPortMessage(pagePort!);
    devtoolsPort.dispatchMessage({ kind: "request", hello: "world" });
    expect(await pageMessagePromise).toEqual({ kind: "request", hello: "world" });

    const runtimeForwardPromise = waitForRuntimeForward(devtoolsPort, (message) =>
      Boolean(
        message &&
        typeof message === "object" &&
        (message as { kind?: unknown }).kind === "response",
      ),
    );
    pagePort!.postMessage({ kind: "response", ok: true });
    expect(await runtimeForwardPromise).toEqual({ kind: "response", ok: true });

    const runtimeEventForwardPromise = waitForRuntimeForward(
      devtoolsPort,
      isEventMessage(config, "client.subscription.delta"),
    );
    fakeWindow.postMessage(
      {
        channel: config.bridgeChannel,
        kind: "event",
        event: "client.subscription.delta",
        payload: { delta: [] },
      },
      "*",
    );
    expect(await runtimeEventForwardPromise).toEqual({
      channel: config.bridgeChannel,
      kind: "event",
      event: "client.subscription.delta",
      payload: { delta: [] },
    });

    const disconnectedEventPromise = waitForWindowMessage(
      fakeWindow,
      isEventMessage(config, config.disconnectedEvent),
    );
    devtoolsPort.disconnect();
    await disconnectedEventPromise;
    pagePort!.close();
  });

  it("only installs the relay once per page", () => {
    const fakeWindow = new FakeWindow();
    const fakeRuntime = new FakeChromeRuntime();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;
    (globalThis as { chrome?: unknown }).chrome = {
      runtime: fakeRuntime,
    };

    const config = createDevtoolsPortRelayConfig();
    installDevtoolsPortRelay(config);
    installDevtoolsPortRelay(config);

    expect(fakeRuntime.listenerCount()).toBe(1);
  });
});

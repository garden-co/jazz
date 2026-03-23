import * as Comlink from "comlink";
import { afterEach, describe, expect, it, vi } from "vitest";
import { attachDevTools } from "./dev-tools.js";
import { DEVTOOLS_MC_CHANNEL, type DevtoolsEvent, type DevtoolsRuntimeAPI } from "./protocol.js";
import type { ActiveQuerySubscriptionTrace } from "../runtime/db.js";

type MessageListener = (event: {
  source: typeof window;
  data: unknown;
  ports?: MessagePort[];
}) => void;

class FakeWindow {
  private readonly listeners = new Set<MessageListener>();

  addEventListener(type: string, listener: MessageListener): void {
    if (type === "message") this.listeners.add(listener);
  }

  removeEventListener(type: string, listener: MessageListener): void {
    if (type === "message") this.listeners.delete(listener);
  }

  postMessage(data: unknown, _origin?: string, transfer?: Transferable[]): void {
    const ports = (transfer ?? []).filter((t): t is MessagePort => t instanceof MessagePort);
    for (const listener of Array.from(this.listeners)) {
      listener({ source: this as unknown as typeof window, data, ports });
    }
  }
}

const originalWindow = (globalThis as { window?: unknown }).window;

afterEach(() => {
  vi.restoreAllMocks();
  if (originalWindow === undefined) {
    delete (globalThis as { window?: unknown }).window;
  } else {
    (globalThis as { window?: unknown }).window = originalWindow;
  }
});

function captureRuntimePort(fakeWindow: FakeWindow): Promise<MessagePort> {
  return new Promise((resolve) => {
    const listener: MessageListener = (event) => {
      if (
        event.data &&
        typeof event.data === "object" &&
        (event.data as Record<string, unknown>).channel === DEVTOOLS_MC_CHANNEL &&
        event.ports &&
        event.ports.length > 0
      ) {
        fakeWindow.removeEventListener("message", listener);
        resolve(event.ports[0]!);
      }
    };
    fakeWindow.addEventListener("message", listener);
  });
}

function collectEvents(port: MessagePort): DevtoolsEvent[] {
  const events: DevtoolsEvent[] = [];
  port.addEventListener("message", (event) => {
    const data = event.data;
    if (data && typeof data === "object" && "type" in data) {
      events.push(data as DevtoolsEvent);
    }
  });
  port.start();
  return events;
}

describe("attachDevTools with Comlink", () => {
  it("enables db devMode when attaching", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(function (this: { config: { devMode?: boolean } }, enabled: boolean) {
        this.config.devMode = enabled;
      }),
      clients: new Map([["default", {}]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    await attachDevTools({ db: fakeDb as any }, {} as any);
    expect(fakeDb.setDevMode).toHaveBeenCalledWith(true);
  });

  it("exposes runtime API via Comlink on MessageChannel", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", { getSchema: () => ({ tables: [] }) }]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, { tables: [] } as any);
    const port = await portPromise;

    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);
    const result = await proxy.announce();

    expect(result.ready).toBe(true);
    expect(result.dbConfig).toEqual(expect.objectContaining({ appId: "devtools-test" }));

    proxy[Comlink.releaseProxy]();
    port.close();
  });
});

describe("attachDevTools mutation bridge", () => {
  it("routes insertDurable through Comlink", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const insertedRow = { id: "row-1", values: [{ type: "Text", value: "hello" }] };
    const createDurable = vi.fn(async () => insertedRow);
    const fakeClient = {
      createDurable,
      updateDurable: vi.fn(async () => undefined),
      deleteDurable: vi.fn(async () => undefined),
      unsubscribe: vi.fn(),
      getSchema: () => ({ tables: [] }),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port = await portPromise;

    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);
    const row = await proxy.insertDurable("todos", [{ type: "Text", value: "hello" }], "worker");

    expect(row).toEqual(insertedRow);
    expect(createDurable).toHaveBeenCalledWith("todos", insertedRow.values, { tier: "worker" });

    proxy[Comlink.releaseProxy]();
    port.close();
  });

  it("routes updateDurable through Comlink", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const updateDurable = vi.fn(async () => undefined);
    const fakeClient = {
      createDurable: vi.fn(async () => ({ id: "row-1", values: [] })),
      updateDurable,
      deleteDurable: vi.fn(async () => undefined),
      unsubscribe: vi.fn(),
      getSchema: () => ({ tables: [] }),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port = await portPromise;

    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);
    await proxy.updateDurable(
      "row-1",
      { title: { type: "Text", value: "updated" } as any },
      "edge",
    );

    expect(updateDurable).toHaveBeenCalledWith(
      "row-1",
      { title: { type: "Text", value: "updated" } },
      { tier: "edge" },
    );

    proxy[Comlink.releaseProxy]();
    port.close();
  });

  it("routes deleteDurable through Comlink", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const deleteDurable = vi.fn(async () => undefined);
    const fakeClient = {
      createDurable: vi.fn(async () => ({ id: "row-1", values: [] })),
      updateDurable: vi.fn(async () => undefined),
      deleteDurable,
      unsubscribe: vi.fn(),
      getSchema: () => ({ tables: [] }),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port = await portPromise;

    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);
    await proxy.deleteDurable("row-1", "global");

    expect(deleteDurable).toHaveBeenCalledWith("row-1", { tier: "global" });

    proxy[Comlink.releaseProxy]();
    port.close();
  });

  it("propagates errors from runtime methods", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const fakeClient = {
      createDurable: vi.fn(async () => {
        throw new Error("insert failed");
      }),
      updateDurable: vi.fn(async () => undefined),
      deleteDurable: vi.fn(async () => undefined),
      unsubscribe: vi.fn(),
      getSchema: () => ({ tables: [] }),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port = await portPromise;

    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);
    await expect(proxy.insertDurable("todos", [], undefined)).rejects.toThrow();

    proxy[Comlink.releaseProxy]();
    port.close();
  });
});

describe("attachDevTools subscription bridge", () => {
  it("pushes subscription deltas as event messages", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    let subscribeCallback: ((delta: unknown) => void) | null = null;
    const fakeClient = {
      subscribe: vi.fn((_query: unknown, callback: (delta: unknown) => void) => {
        subscribeCallback = callback;
        return 42;
      }),
      unsubscribe: vi.fn(),
      getSchema: () => ({ tables: [] }),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port = await portPromise;

    const events = collectEvents(port);
    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);

    await proxy.subscribe('{"table":"todos"}', "sub-1", undefined);

    expect(fakeClient.subscribe).toHaveBeenCalled();
    expect(subscribeCallback).not.toBeNull();

    // Trigger a delta
    const delta = [{ op: "insert", row: { id: "1" } }];
    subscribeCallback!(delta);

    // Give the event time to arrive
    await new Promise((r) => setTimeout(r, 50));

    const deltaEvents = events.filter((e) => e.type === "subscription-delta");
    expect(deltaEvents).toHaveLength(1);
    expect(deltaEvents[0]).toEqual({
      type: "subscription-delta",
      subscriptionId: "sub-1",
      delta,
    });

    proxy[Comlink.releaseProxy]();
    port.close();
  });
});

describe("attachDevTools active query subscription bridge", () => {
  it("returns snapshots and pushes updates as event messages", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const initialSubscriptions: ActiveQuerySubscriptionTrace[] = [
      {
        id: "sub-1",
        query: '{"table":"todos"}',
        table: "todos",
        branches: ["main"],
        tier: "worker",
        propagation: "full",
        createdAt: "2026-03-10T10:00:00.000Z",
        stack: "Error\n  at demo",
      },
    ];
    const nextSubscriptions: ActiveQuerySubscriptionTrace[] = [
      { ...initialSubscriptions[0]!, id: "sub-2" },
    ];

    let currentSubscriptions = initialSubscriptions;
    let notifyChange: ((subs: readonly ActiveQuerySubscriptionTrace[]) => void) | null = null;

    const fakeDb = {
      config: { appId: "devtools-test", devMode: true },
      setDevMode: vi.fn(),
      clients: new Map([["default", { getSchema: () => ({ tables: [] }) }]]),
      getActiveQuerySubscriptions: vi.fn(() => currentSubscriptions),
      onActiveQuerySubscriptionsChange: vi.fn(
        (listener: (subs: readonly ActiveQuerySubscriptionTrace[]) => void) => {
          notifyChange = listener;
          return () => {
            notifyChange = null;
          };
        },
      ),
    };

    const portPromise = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port = await portPromise;

    const events = collectEvents(port);
    const proxy = Comlink.wrap<DevtoolsRuntimeAPI>(port);

    // Announce triggers the active query subscription listener setup
    await proxy.announce();

    const listed = await proxy.listActiveQuerySubscriptions();
    expect(listed).toEqual(initialSubscriptions);

    // Trigger a change
    currentSubscriptions = nextSubscriptions;
    notifyChange!(nextSubscriptions);

    await new Promise((r) => setTimeout(r, 50));

    const changeEvents = events.filter((e) => e.type === "active-query-subscriptions-changed");
    expect(changeEvents.length).toBeGreaterThanOrEqual(1);
    const lastEvent = changeEvents[changeEvents.length - 1]!;
    if (lastEvent.type === "active-query-subscriptions-changed") {
      expect(lastEvent.subscriptions).toEqual(nextSubscriptions);
    }

    proxy[Comlink.releaseProxy]();
    port.close();
  });
});

describe("attachDevTools disconnect cleanup", () => {
  it("cleans up subscriptions when a new connection replaces the old one", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const unsubscribe = vi.fn();
    let subscribeCallback: ((delta: unknown) => void) | null = null;
    const fakeClient = {
      subscribe: vi.fn((_query: unknown, callback: (delta: unknown) => void) => {
        subscribeCallback = callback;
        return 42;
      }),
      unsubscribe,
      getSchema: () => ({ tables: [] }),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    // First connection
    const portPromise1 = captureRuntimePort(fakeWindow);
    await attachDevTools({ db: fakeDb as any }, {} as any);
    const port1 = await portPromise1;

    const proxy1 = Comlink.wrap<DevtoolsRuntimeAPI>(port1);
    await proxy1.subscribe('{"table":"todos"}', "sub-1", undefined);
    proxy1[Comlink.releaseProxy]();

    // Simulate reconnection — content script requests a new port
    const portPromise2 = captureRuntimePort(fakeWindow);
    fakeWindow.postMessage({ channel: DEVTOOLS_MC_CHANNEL, kind: "request-port" });
    const port2 = await portPromise2;

    // Old subscriptions should have been cleaned up
    expect(unsubscribe).toHaveBeenCalledWith(42);

    const proxy2 = Comlink.wrap<DevtoolsRuntimeAPI>(port2);
    const result = await proxy2.announce();
    expect(result).toBeDefined();

    proxy2[Comlink.releaseProxy]();
    port2.close();
  });
});

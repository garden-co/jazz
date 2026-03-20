import { afterEach, describe, expect, it, vi } from "vitest";
import { wrap, type Remote } from "comlink";
import { attachDevTools } from "./dev-tools.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_CONTROL_MESSAGES,
  DEVTOOLS_EVENTS,
  type DevtoolsBridgeApi,
  type DevtoolsControlMessage,
  type DevtoolsEventEnvelope,
} from "./protocol.js";
import type { ActiveQuerySubscriptionTrace } from "../runtime/db.js";

type MessageListener = (event: {
  source: FakeWindow;
  data: unknown;
  ports: readonly MessagePort[];
}) => void;

class FakeWindow {
  private readonly listeners = new Set<MessageListener>();

  addEventListener(type: string, listener: MessageListener): void {
    if (type === "message") {
      this.listeners.add(listener);
    }
  }

  removeEventListener(type: string, listener: MessageListener): void {
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

const originalWindow = (globalThis as { window?: unknown }).window;

afterEach(() => {
  vi.restoreAllMocks();
  if (originalWindow === undefined) {
    delete (globalThis as { window?: unknown }).window;
  } else {
    (globalThis as { window?: unknown }).window = originalWindow;
  }
});

function waitForEvent(
  fakeWindow: FakeWindow,
  eventName: string,
): Promise<
  DevtoolsEventEnvelope<typeof DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED>
> {
  return new Promise((resolve) => {
    const listener: MessageListener = (event) => {
      const message = event.data as Partial<DevtoolsEventEnvelope>;
      if (
        message.channel !== DEVTOOLS_BRIDGE_CHANNEL ||
        message.kind !== "event" ||
        message.event !== eventName
      ) {
        return;
      }
      fakeWindow.removeEventListener("message", listener);
      resolve(
        message as DevtoolsEventEnvelope<
          typeof DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED
        >,
      );
    };
    fakeWindow.addEventListener("message", listener);
  });
}

async function connectBridge(
  fakeWindow: FakeWindow,
): Promise<{ bridge: Remote<DevtoolsBridgeApi>; dispose: () => void }> {
  const channel = new MessageChannel();
  channel.port1.start?.();
  channel.port2.start?.();

  await new Promise<void>((resolve) => {
    const onReadyMessage = (event: MessageEvent) => {
      const message = event.data as Partial<DevtoolsControlMessage>;
      if (
        message.channel !== DEVTOOLS_BRIDGE_CHANNEL ||
        message.kind !== DEVTOOLS_CONTROL_MESSAGES.COMLINK_READY
      ) {
        return;
      }

      channel.port1.removeEventListener("message", onReadyMessage);
      resolve();
    };

    channel.port1.addEventListener("message", onReadyMessage);
    fakeWindow.postMessage(
      {
        channel: DEVTOOLS_BRIDGE_CHANNEL,
        kind: DEVTOOLS_CONTROL_MESSAGES.COMLINK_CONNECT,
      },
      "*",
      [channel.port2],
    );
  });

  return {
    bridge: wrap<DevtoolsBridgeApi>(channel.port1),
    dispose() {
      channel.port1.close();
    },
  };
}

describe("attachDevTools active query subscription bridge", () => {
  it("enables db devMode when attaching", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const fakeDb = {
      config: {
        appId: "devtools-test",
      },
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

  it("returns snapshots and pushes updates", async () => {
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
      {
        ...initialSubscriptions[0]!,
        id: "sub-2",
      },
    ];

    let currentSubscriptions = initialSubscriptions;
    let notifySubscriptionsChange:
      | ((subscriptions: readonly ActiveQuerySubscriptionTrace[]) => void)
      | null = null;

    const fakeDb = {
      config: {
        appId: "devtools-test",
        devMode: true,
      },
      setDevMode: vi.fn(),
      clients: new Map([["default", {}]]),
      getActiveQuerySubscriptions: vi.fn(() => currentSubscriptions),
      onActiveQuerySubscriptionsChange: vi.fn(
        (listener: (subscriptions: readonly ActiveQuerySubscriptionTrace[]) => void) => {
          notifySubscriptionsChange = listener;
          listener(currentSubscriptions);
          return () => {
            notifySubscriptionsChange = null;
          };
        },
      ),
    };

    await attachDevTools({ db: fakeDb as any }, {} as any);

    const { bridge, dispose } = await connectBridge(fakeWindow);
    expect((await bridge.announce()).ready).toBe(true);
    expect(await bridge.listActiveQuerySubscriptions()).toEqual(initialSubscriptions);

    const eventPromise = waitForEvent(
      fakeWindow,
      DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED,
    );
    currentSubscriptions = nextSubscriptions;
    (
      notifySubscriptionsChange as
        | ((subscriptions: readonly ActiveQuerySubscriptionTrace[]) => void)
        | null
    )?.(nextSubscriptions);

    expect((await eventPromise).payload.subscriptions).toEqual(nextSubscriptions);
    dispose();
  });
});

describe("attachDevTools mutation bridge", () => {
  it("routes client.insertDurable to runtime createDurable", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const insertedRow = {
      id: "row-1",
      values: [{ type: "Text", value: "hello" }],
    };
    const createDurable = vi.fn(async () => insertedRow);
    const fakeClient = {
      createDurable,
      updateDurable: vi.fn(async () => undefined),
      deleteDurable: vi.fn(async () => undefined),
      unsubscribe: vi.fn(),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    await attachDevTools({ db: fakeDb as any }, {} as any);

    const { bridge, dispose } = await connectBridge(fakeWindow);
    const response = await bridge.insertDurable({
      table: "todos",
      values: [{ type: "Text", value: "hello" }],
      tier: "worker",
    });

    expect(response).toEqual(insertedRow);
    expect(createDurable).toHaveBeenCalledWith("todos", insertedRow.values, { tier: "worker" });
    dispose();
  });

  it("routes client.updateDurable to runtime updateDurable", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const updateDurable = vi.fn(async () => undefined);
    const fakeClient = {
      createDurable: vi.fn(async () => ({ id: "row-1", values: [] })),
      updateDurable,
      deleteDurable: vi.fn(async () => undefined),
      unsubscribe: vi.fn(),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    await attachDevTools({ db: fakeDb as any }, {} as any);

    const { bridge, dispose } = await connectBridge(fakeWindow);
    const response = await bridge.updateDurable({
      objectId: "row-1",
      updates: {
        title: { type: "Text", value: "updated" },
      },
      tier: "edge",
    });

    expect(response).toEqual({ updated: true });
    expect(updateDurable).toHaveBeenCalledWith(
      "row-1",
      { title: { type: "Text", value: "updated" } },
      { tier: "edge" },
    );
    dispose();
  });

  it("routes client.deleteDurable to runtime deleteDurable", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const deleteDurable = vi.fn(async () => undefined);
    const fakeClient = {
      createDurable: vi.fn(async () => ({ id: "row-1", values: [] })),
      updateDurable: vi.fn(async () => undefined),
      deleteDurable,
      unsubscribe: vi.fn(),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    await attachDevTools({ db: fakeDb as any }, {} as any);

    const { bridge, dispose } = await connectBridge(fakeWindow);
    const response = await bridge.deleteDurable({
      objectId: "row-1",
      tier: "global",
    });

    expect(response).toEqual({ deleted: true });
    expect(deleteDurable).toHaveBeenCalledWith("row-1", { tier: "global" });
    dispose();
  });

  it("returns command-specific errors for invalid mutation payloads", async () => {
    const fakeWindow = new FakeWindow();
    (globalThis as { window?: unknown }).window = fakeWindow as unknown;

    const fakeClient = {
      createDurable: vi.fn(async () => ({ id: "row-1", values: [] })),
      updateDurable: vi.fn(async () => undefined),
      deleteDurable: vi.fn(async () => undefined),
      unsubscribe: vi.fn(),
    };
    const fakeDb = {
      config: { appId: "devtools-test" },
      setDevMode: vi.fn(),
      clients: new Map([["default", fakeClient]]),
      getActiveQuerySubscriptions: vi.fn(() => []),
      onActiveQuerySubscriptionsChange: vi.fn(() => () => {}),
    };

    await attachDevTools({ db: fakeDb as any }, {} as any);

    const { bridge, dispose } = await connectBridge(fakeWindow);

    await expect(
      bridge.insertDurable({
        table: 123 as unknown as string,
        values: [],
      } as any),
    ).rejects.toThrow("Invalid payload for client.insertDurable.");

    await expect(
      bridge.updateDurable({
        objectId: "row-1",
        updates: null,
      } as any),
    ).rejects.toThrow("Invalid payload for client.updateDurable.");

    await expect(
      bridge.deleteDurable({
        objectId: 123 as unknown as string,
      } as any),
    ).rejects.toThrow("Invalid payload for client.deleteDurable.");

    dispose();
  });
});

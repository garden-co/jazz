import { afterEach, describe, expect, it, vi } from "vitest";
import { attachDevTools } from "./dev-tools.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_COMMANDS,
  DEVTOOLS_EVENTS,
  type DevtoolsEventEnvelope,
  type DevtoolsResponseEnvelope,
} from "./protocol.js";
import type { ActiveQuerySubscriptionTrace } from "../runtime/db.js";

type MessageListener = (event: { source: FakeWindow; data: unknown }) => void;

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

  postMessage(data: unknown): void {
    for (const listener of Array.from(this.listeners)) {
      listener({ source: this, data });
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

function waitForResponse(
  fakeWindow: FakeWindow,
  requestId: string,
): Promise<DevtoolsResponseEnvelope> {
  return new Promise((resolve) => {
    const listener: MessageListener = (event) => {
      const message = event.data as Partial<DevtoolsResponseEnvelope>;
      if (
        message.channel !== DEVTOOLS_BRIDGE_CHANNEL ||
        message.kind !== "response" ||
        message.requestId !== requestId
      ) {
        return;
      }
      fakeWindow.removeEventListener("message", listener);
      resolve(message as DevtoolsResponseEnvelope);
    };
    fakeWindow.addEventListener("message", listener);
  });
}

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

    const announceRequestId = "announce-1";
    const announceResponsePromise = waitForResponse(fakeWindow, announceRequestId);
    fakeWindow.postMessage({
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "request",
      requestId: announceRequestId,
      command: DEVTOOLS_COMMANDS.ANNOUNCE,
      payload: {},
    });

    expect((await announceResponsePromise).ok).toBe(true);

    const listRequestId = "list-1";
    const listResponsePromise = waitForResponse(fakeWindow, listRequestId);
    fakeWindow.postMessage({
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "request",
      requestId: listRequestId,
      command: DEVTOOLS_COMMANDS.CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS,
      payload: {},
    });

    expect((await listResponsePromise).payload).toEqual(initialSubscriptions);

    const eventPromise = waitForEvent(
      fakeWindow,
      DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED,
    );
    currentSubscriptions = nextSubscriptions;
    if (notifySubscriptionsChange) {
      // @ts-expect-error
      notifySubscriptionsChange(nextSubscriptions);
    }

    expect((await eventPromise).payload.subscriptions).toEqual(nextSubscriptions);
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

    const requestId = "insert-1";
    const responsePromise = waitForResponse(fakeWindow, requestId);
    fakeWindow.postMessage({
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "request",
      requestId,
      command: DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE,
      payload: {
        table: "todos",
        values: [{ type: "Text", value: "hello" }],
        tier: "worker",
      },
    });

    const response = await responsePromise;
    expect(response.ok).toBe(true);
    expect(response.payload).toEqual(insertedRow);
    expect(createDurable).toHaveBeenCalledWith("todos", insertedRow.values, { tier: "worker" });
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

    const requestId = "update-1";
    const responsePromise = waitForResponse(fakeWindow, requestId);
    fakeWindow.postMessage({
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "request",
      requestId,
      command: DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE,
      payload: {
        objectId: "row-1",
        updates: {
          title: { type: "Text", value: "updated" },
        },
        tier: "edge",
      },
    });

    const response = await responsePromise;
    expect(response.ok).toBe(true);
    expect(response.payload).toEqual({ updated: true });
    expect(updateDurable).toHaveBeenCalledWith(
      "row-1",
      { title: { type: "Text", value: "updated" } },
      { tier: "edge" },
    );
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

    const requestId = "delete-1";
    const responsePromise = waitForResponse(fakeWindow, requestId);
    fakeWindow.postMessage({
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "request",
      requestId,
      command: DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE,
      payload: {
        objectId: "row-1",
        tier: "global",
      },
    });

    const response = await responsePromise;
    expect(response.ok).toBe(true);
    expect(response.payload).toEqual({ deleted: true });
    expect(deleteDurable).toHaveBeenCalledWith("row-1", { tier: "global" });
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

    const invalidCases = [
      {
        requestId: "invalid-insert",
        command: DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE,
        payload: { table: 123, values: [] },
        expectedMessage: "Invalid payload for client.insertDurable.",
      },
      {
        requestId: "invalid-update",
        command: DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE,
        payload: { objectId: "row-1", updates: null },
        expectedMessage: "Invalid payload for client.updateDurable.",
      },
      {
        requestId: "invalid-delete",
        command: DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE,
        payload: { objectId: 123 },
        expectedMessage: "Invalid payload for client.deleteDurable.",
      },
    ];

    for (const testCase of invalidCases) {
      const responsePromise = waitForResponse(fakeWindow, testCase.requestId);
      fakeWindow.postMessage({
        channel: DEVTOOLS_BRIDGE_CHANNEL,
        kind: "request",
        requestId: testCase.requestId,
        command: testCase.command,
        payload: testCase.payload,
      });

      const response = await responsePromise;
      expect(response.ok).toBe(false);
      expect(response.error?.message).toBe(testCase.expectedMessage);
    }
  });
});

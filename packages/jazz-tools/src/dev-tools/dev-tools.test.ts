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
        ...initialSubscriptions[0],
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

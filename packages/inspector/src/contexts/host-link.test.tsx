import { afterEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { INSPECTOR_HOST_GLOBAL } from "jazz-tools";
import {
  closeInspectorRuntimePort,
  openInspectorRuntimeSession,
  readInspectorHostConfig,
  readInspectorHostSchema,
  useHostSubscriptions,
} from "./host-link.js";

type TestPort = MessagePort & { dispatch(data: unknown): void };

function createTestPort() {
  const listeners = new Set<(event: MessageEvent) => void>();
  const postMessage = vi.fn();
  const port = {
    addEventListener: vi.fn((type: string, listener: unknown) => {
      if (type === "message") listeners.add(listener as (event: MessageEvent) => void);
    }),
    removeEventListener: vi.fn((type: string, listener: unknown) => {
      if (type === "message") listeners.delete(listener as (event: MessageEvent) => void);
    }),
    start: vi.fn(),
    postMessage,
    close: vi.fn(),
    dispatch(data: unknown) {
      for (const listener of listeners) listener(new MessageEvent("message", { data }));
    },
  } as unknown as TestPort;
  return { port, postMessage };
}

// In jsdom, window.parent === window, so installing on window.parent installs here.
function installHost(
  subs: unknown[] = [],
  openControlPort?: (signal?: AbortSignal) => Promise<MessagePort>,
) {
  (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = {
    getConnectionConfig: () => ({
      appId: "app1",
      serverUrl: "http://server",
      env: "dev",
      adminSecret: "sek",
    }),
    getWasmSchema: () => ({ todos: { columns: [] } }),
    getActiveSubscriptions: () => subs,
    openControlPort,
  };
}

afterEach(() => {
  delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  Object.defineProperty(window, "opener", { configurable: true, value: null });
});

describe("host-link", () => {
  it("reads config + schema from the host handle, null when absent", () => {
    expect(readInspectorHostConfig()).toBeNull();
    installHost();
    expect(readInspectorHostConfig()).toMatchObject({ appId: "app1", serverUrl: "http://server" });
    expect(readInspectorHostSchema()).toEqual({ todos: { columns: [] } });
  });

  it("seeds subscriptions from the handle and updates on push", () => {
    installHost([{ id: "s1", table: "todos" }]);
    const { result } = renderHook(() => useHostSubscriptions());
    expect(result.current).toEqual([{ id: "s1", table: "todos" }]);

    act(() => {
      window.dispatchEvent(
        new MessageEvent("message", {
          data: { type: "jazz-inspector:subscriptions", list: [{ id: "s2", table: "projects" }] },
          origin: window.location.origin,
          // Real cross-window postMessage sets event.source to the sender;
          // window.parent === window in jsdom, so that's `window` here.
          source: window,
        }),
      );
    });
    expect(result.current).toEqual([{ id: "s2", table: "projects" }]);
  });

  it("ignores a subscriptions push whose source isn't the host window", () => {
    installHost([{ id: "s1", table: "todos" }]);
    const { result } = renderHook(() => useHostSubscriptions());

    act(() => {
      window.dispatchEvent(
        new MessageEvent("message", {
          data: { type: "jazz-inspector:subscriptions", list: [{ id: "s2", table: "projects" }] },
        }),
      );
    });
    expect(result.current).toEqual([{ id: "s1", table: "todos" }]);
  });

  it("returns [] when there is no host", () => {
    const { result } = renderHook(() => useHostSubscriptions());
    expect(result.current).toEqual([]);
  });

  it("ignores unrelated messages", () => {
    installHost([]);
    const { result } = renderHook(() => useHostSubscriptions());
    act(() => {
      window.dispatchEvent(new MessageEvent("message", { data: { type: "other" } }));
    });
    expect(result.current).toEqual([]);
  });

  it("reads from and registers a detached window with its opener host", () => {
    const registerInspectorWindow = vi.fn();
    const unregisterInspectorWindow = vi.fn();
    const opener = {
      [INSPECTOR_HOST_GLOBAL]: {
        getConnectionConfig: () => ({ appId: "detached" }),
        getWasmSchema: () => ({ todos: { columns: [] } }),
        getActiveSubscriptions: () => [{ id: "s1", table: "todos" }],
        registerInspectorWindow,
        unregisterInspectorWindow,
      },
    } as unknown as Window;
    const previousOpener = window.opener;
    Object.defineProperty(window, "opener", { configurable: true, value: opener });

    const { result, unmount } = renderHook(() => useHostSubscriptions());

    expect(readInspectorHostConfig()).toMatchObject({ appId: "detached" });
    expect(result.current).toEqual([{ id: "s1", table: "todos" }]);
    expect(registerInspectorWindow).toHaveBeenCalledWith(window);

    act(() => {
      window.dispatchEvent(
        new MessageEvent("message", {
          data: { type: "jazz-inspector:subscriptions", list: [{ id: "s2", table: "projects" }] },
          origin: window.location.origin,
          source: opener,
        }),
      );
    });
    expect(result.current).toEqual([{ id: "s2", table: "projects" }]);

    unmount();
    expect(unregisterInspectorWindow).toHaveBeenCalledWith(window);
    Object.defineProperty(window, "opener", { configurable: true, value: previousOpener });
  });

  it("falls back to the parent host when an opener's parent is cross-origin", () => {
    installHost();
    const opener = Object.create(null, {
      parent: {
        get() {
          throw new DOMException("Blocked", "SecurityError");
        },
      },
    }) as Window;
    Object.defineProperty(window, "opener", { configurable: true, value: opener });

    expect(readInspectorHostConfig()).toMatchObject({ appId: "app1" });
  });
  it("protocol-closes runtime ports", async () => {
    const channel = new MessageChannel();
    channel.port2.start();
    const closed = new Promise<unknown>((resolve) => {
      channel.port2.addEventListener("message", (event) => resolve(event.data), { once: true });
    });

    closeInspectorRuntimePort(channel.port1);

    await expect(closed).resolves.toEqual({ type: "close" });
    channel.port2.close();
  });

  it("aborts the host control opening at the absolute deadline", async () => {
    vi.useFakeTimers();
    let openingSignal: AbortSignal | undefined;
    try {
      installHost(
        [],
        (signal) =>
          new Promise<MessagePort>(() => {
            openingSignal = signal;
          }),
      );
      const opening = openInspectorRuntimeSession({ deadline: Date.now() + 100 }).then(
        () => ({ kind: "resolved" as const }),
        (error: unknown) => ({ kind: "rejected" as const, error }),
      );

      await vi.advanceTimersByTimeAsync(100);

      const result = await opening;
      expect(result.kind).toBe("rejected");
      if (result.kind === "rejected") {
        expect(result.error).toMatchObject({ message: expect.stringContaining("cancelled") });
      }
      expect(openingSignal?.aborted).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("propagates an external abort into host control opening", async () => {
    const controller = new AbortController();
    let openingSignal: AbortSignal | undefined;
    installHost(
      [],
      (signal) =>
        new Promise<MessagePort>(() => {
          openingSignal = signal;
        }),
    );
    const opening = openInspectorRuntimeSession({ signal: controller.signal });

    controller.abort();

    await expect(opening).rejects.toThrow("cancelled");
    expect(openingSignal?.aborted).toBe(true);
  });

  it("bounds and cleans up a silent control request", async () => {
    vi.useFakeTimers();
    try {
      const control = createTestPort();
      installHost([], async () => control.port);
      const opening = openInspectorRuntimeSession().then(
        () => ({ kind: "resolved" as const }),
        (error: unknown) => ({ kind: "rejected" as const, error }),
      );
      let settled = false;
      void opening.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );

      await vi.advanceTimersByTimeAsync(4_999);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const result = await opening;
      expect(result.kind).toBe("rejected");
      if (result.kind === "rejected") expect(result.error).toBeInstanceOf(Error);
      expect(control.port.removeEventListener).toHaveBeenCalledOnce();
      expect(control.port.close).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it("closes the attached port when the host rejects attachment", async () => {
    const control = createTestPort();
    control.postMessage.mockImplementation((message: { id: number; type: string }) => {
      if (message.type === "list-contexts") {
        control.port.dispatch({
          id: message.id,
          type: "contexts",
          contexts: [{ key: "ctx", appId: "app1", dbName: "db", schema: {} }],
        });
      } else if (message.type === "attach-context") {
        control.port.dispatch({
          id: message.id,
          type: "result",
          error: {
            name: "InspectorAttachError",
            message: "attach failed",
            code: "context_unavailable",
            cause: { name: "Error", message: "context disappeared" },
          },
        });
      }
    });
    installHost([], async () => control.port);

    const attachedPort = createTestPort();
    const previousMessageChannel = globalThis.MessageChannel;
    Object.defineProperty(globalThis, "MessageChannel", {
      configurable: true,
      value: class {
        port1 = attachedPort.port;
        port2 = createTestPort().port;
      },
    });
    try {
      const session = await openInspectorRuntimeSession();
      await expect(session?.attach("ctx")).rejects.toMatchObject({
        name: "InspectorAttachError",
        message: "attach failed",
        code: "context_unavailable",
        cause: { message: "context disappeared" },
      });
      expect(attachedPort.port.close).toHaveBeenCalledOnce();
    } finally {
      Object.defineProperty(globalThis, "MessageChannel", {
        configurable: true,
        value: previousMessageChannel,
      });
    }
  });

  it("closes a control session idempotently", async () => {
    const control = createTestPort();
    control.postMessage.mockImplementation((message: { id: number; type: string }) => {
      if (message.type === "list-contexts") {
        control.port.dispatch({ id: message.id, type: "contexts", contexts: [] });
      }
    });
    installHost([], async () => control.port);

    const session = await openInspectorRuntimeSession();
    session?.close();
    session?.close();

    expect(control.postMessage).toHaveBeenCalledWith({ type: "close" });
    expect(
      control.postMessage.mock.calls.filter(([message]) => message.type === "close"),
    ).toHaveLength(1);
    expect(control.port.close).toHaveBeenCalledOnce();
  });
});

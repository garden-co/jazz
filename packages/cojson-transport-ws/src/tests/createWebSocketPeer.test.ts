import type { CojsonInternalTypes, SyncMessage } from "cojson";
import { cojsonInternals, logger } from "cojson";
import { type Mocked, afterEach, describe, expect, test, vi } from "vitest";
import {
  type CreateWebSocketPeerOpts,
  createWebSocketPeer,
} from "../createWebSocketPeer.js";
import type { AnyWebSocket } from "../types.js";
import { BUFFER_LIMIT, BUFFER_LIMIT_POLLING_INTERVAL } from "../utils.js";
import { createTestMetricReader, tearDownTestMetricReader } from "./utils.js";

const { CO_VALUE_PRIORITY, WEBSOCKET_CONFIG } = cojsonInternals;

const { MAX_OUTGOING_MESSAGES_CHUNK_BYTES } = WEBSOCKET_CONFIG;

interface SetupOptions extends Partial<CreateWebSocketPeerOpts> {
  initialReadyState?: number;
}

function setup(opts: SetupOptions = {}) {
  const { initialReadyState = 1, ...peerOpts } = opts;
  const listeners = new Map<
    string,
    Set<{ callback: (event: MessageEvent) => void; once?: boolean }>
  >();

  const mockWebSocket = {
    readyState: initialReadyState,
    bufferedAmount: 0,
    addEventListener: vi
      .fn()
      .mockImplementation(
        (
          type: string,
          callback: (event: MessageEvent) => void,
          options?: { once?: boolean },
        ) => {
          if (!listeners.has(type)) {
            listeners.set(type, new Set());
          }
          listeners.get(type)!.add({ callback, once: options?.once });
        },
      ),
    removeEventListener: vi
      .fn()
      .mockImplementation(
        (type: string, callback: (event: MessageEvent) => void) => {
          const set = listeners.get(type);
          if (set) {
            for (const entry of set) {
              if (entry.callback === callback) {
                set.delete(entry);
                break;
              }
            }
          }
        },
      ),
    close: vi.fn(),
    send: vi.fn(),
  } as unknown as Mocked<AnyWebSocket>;

  const triggerEvent = (type: string, event?: MessageEvent) => {
    const set = listeners.get(type);
    if (set) {
      const toRemove: {
        callback: (event: MessageEvent) => void;
        once?: boolean;
      }[] = [];
      for (const entry of set) {
        entry.callback(event ?? new MessageEvent(type));
        if (entry.once) {
          toRemove.push(entry);
        }
      }
      for (const entry of toRemove) {
        set.delete(entry);
      }
    }
  };

  const peer = createWebSocketPeer({
    id: "test-peer",
    websocket: mockWebSocket,
    role: "client",
    batchingByDefault: true,
    ...peerOpts,
  });

  return { mockWebSocket, peer, listeners, triggerEvent };
}

function serializeMessages(messages: SyncMessage[]) {
  return messages.map((msg) => JSON.stringify(msg)).join("\n");
}

describe("createWebSocketPeer", () => {
  test("should create a peer with correct properties", () => {
    const { peer } = setup();

    expect(peer).toHaveProperty("id", "test-peer");
    expect(peer).toHaveProperty("incoming");
    expect(peer).toHaveProperty("outgoing");
    expect(peer).toHaveProperty("role", "client");
  });

  test("should handle disconnection", async () => {
    const { triggerEvent, peer } = setup();

    const onMessageSpy = vi.fn();
    peer.incoming.onMessage(onMessageSpy);

    triggerEvent("close");

    expect(onMessageSpy).toHaveBeenCalledWith("Disconnected");
  });

  test("should handle ping timeout", async () => {
    vi.useFakeTimers();
    const { triggerEvent, peer } = setup();

    const onMessageSpy = vi.fn();

    peer.incoming.onMessage(onMessageSpy);

    triggerEvent("message", new MessageEvent("message", { data: "{}" }));

    await vi.advanceTimersByTimeAsync(10_000);

    expect(onMessageSpy).toHaveBeenCalledWith("Disconnected");

    vi.useRealTimers();
  });

  test("should log ping timeout with peer metadata", async () => {
    vi.useFakeTimers();
    const warnSpy = vi.spyOn(logger, "warn").mockImplementation(() => {});
    const { triggerEvent } = setup({
      id: "peer-123",
      role: "server",
    });

    triggerEvent("message", new MessageEvent("message", { data: "{}" }));

    await vi.advanceTimersByTimeAsync(10_000);

    expect(warnSpy).toHaveBeenCalledWith("Ping timeout from peer", {
      peerId: "peer-123",
      peerRole: "server",
    });

    warnSpy.mockRestore();
    vi.useRealTimers();
  });

  test("should log ping delay when enabled", () => {
    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(1_000);
    const infoSpy = vi.spyOn(logger, "info").mockImplementation(() => {});
    const { triggerEvent } = setup({
      enablePingDelayLogs: true,
      pingDelayLogsData: {
        source: "test-suite",
      },
    });

    triggerEvent(
      "message",
      new MessageEvent("message", {
        data: JSON.stringify({
          type: "ping",
          time: 750,
          dc: "unknown",
        }),
      }),
    );

    expect(infoSpy).toHaveBeenCalledWith("Ping delay", {
      delay: 250,
      server: "unknown",
      source: "test-suite",
    });

    infoSpy.mockRestore();
    nowSpy.mockRestore();
  });

  test("should extend ping timeout when receiving new messages", async () => {
    vi.useFakeTimers();
    const { triggerEvent, peer } = setup({ pingTimeout: 1_000 });

    const onMessageSpy = vi.fn();

    peer.incoming.onMessage(onMessageSpy);

    triggerEvent("message", new MessageEvent("message", { data: "{}" }));

    await vi.advanceTimersByTimeAsync(900);

    triggerEvent("message", new MessageEvent("message", { data: "{}" }));

    await vi.advanceTimersByTimeAsync(900);

    expect(onMessageSpy).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(100);

    expect(onMessageSpy).toHaveBeenCalledWith("Disconnected");

    vi.useRealTimers();
  });

  test("should send outgoing messages", async () => {
    const { peer, mockWebSocket } = setup();

    const testMessage: SyncMessage = {
      action: "known",
      id: "co_ztest",
      header: false,
      sessions: {},
    };

    peer.outgoing.push(testMessage);

    await waitFor(() => {
      expect(mockWebSocket.send).toHaveBeenCalledWith(
        JSON.stringify(testMessage),
      );
    });
  });

  test("should stop sending messages when the websocket is closed", async () => {
    const { peer, mockWebSocket } = setup();

    mockWebSocket.send.mockImplementation(() => {
      mockWebSocket.readyState = 0;
    });

    const message1: SyncMessage = {
      action: "known",
      id: "co_ztest",
      header: false,
      sessions: {},
    };

    const message2: SyncMessage = {
      action: "content",
      id: "co_zlow",
      new: {},
      priority: 6,
    };

    void peer.outgoing.push(message1);

    await waitFor(() => {
      expect(mockWebSocket.send).toHaveBeenCalled();
    });

    expect(mockWebSocket.send).toHaveBeenCalledWith(JSON.stringify(message1));

    mockWebSocket.send.mockClear();
    void peer.outgoing.push(message2);

    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    expect(mockWebSocket.send).not.toHaveBeenCalled();
  });

  test("should close the websocket connection", () => {
    const { mockWebSocket, peer } = setup();

    peer.outgoing.close();

    expect(mockWebSocket.close).toHaveBeenCalled();
  });

  test("should call onSuccess handler after receiving first message", () => {
    const onSuccess = vi.fn();
    const { triggerEvent } = setup({ onSuccess });

    const message: SyncMessage = {
      action: "known",
      id: "co_ztest",
      header: false,
      sessions: {},
    };

    // First message should trigger onSuccess
    triggerEvent(
      "message",
      new MessageEvent("message", { data: JSON.stringify(message) }),
    );
    expect(onSuccess).toHaveBeenCalledTimes(1);

    // Subsequent messages should not trigger onSuccess again
    triggerEvent(
      "message",
      new MessageEvent("message", { data: JSON.stringify(message) }),
    );
    expect(onSuccess).toHaveBeenCalledTimes(1);
  });

  describe("batchingByDefault = true", () => {
    test("should batch outgoing messages when socket is not ready", async () => {
      const { peer, mockWebSocket, triggerEvent } = setup({
        initialReadyState: 0,
      });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      void peer.outgoing.push(message1);
      void peer.outgoing.push(message2);

      // Simulate socket becoming ready
      mockWebSocket.readyState = 1;
      triggerEvent("open");

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message1, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );
    });

    test("should send messages immediately when socket is ready", async () => {
      const { peer, mockWebSocket } = setup();

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      void peer.outgoing.push(message1);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(1);
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(JSON.stringify(message1));

      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(2);
      });

      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        2,
        JSON.stringify(message2),
      );
    });

    test("should sort remaining queued messages by priority after first message", async () => {
      const { peer, mockWebSocket, triggerEvent } = setup({
        initialReadyState: 0,
      });

      const lowPriority: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: CO_VALUE_PRIORITY.LOW,
      };

      const highPriority: SyncMessage = {
        action: "content",
        id: "co_zhigh",
        new: {},
        priority: CO_VALUE_PRIORITY.HIGH,
      };

      // First message is pulled immediately before socket check,
      // so it will be first regardless of priority
      void peer.outgoing.push(lowPriority);
      // Subsequent messages are queued and sorted by priority
      void peer.outgoing.push(lowPriority);
      void peer.outgoing.push(highPriority);

      // Simulate socket becoming ready
      mockWebSocket.readyState = 1;
      triggerEvent("open");

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });

      // First message (lowPriority) comes first as it was pulled before waiting,
      // then remaining messages are sorted: highPriority before lowPriority
      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [lowPriority, highPriority, lowPriority]
          .map((msg) => JSON.stringify(msg))
          .join("\n"),
      );
    });

    test("should send remaining queued messages when close is called", async () => {
      const { peer, mockWebSocket } = setup({ initialReadyState: 0 });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      const message3: SyncMessage = {
        action: "content",
        id: "co_zmedium",
        new: {},
        priority: 3,
      };

      void peer.outgoing.push(message1);
      void peer.outgoing.push(message2);
      void peer.outgoing.push(message3);

      // Set socket to open before close to allow sending
      mockWebSocket.readyState = 1;
      peer.outgoing.close();

      // First message was already pulled by processQueue (waiting for socket),
      // close() processes and sends remaining messages from queue sorted by priority
      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message3, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );
    });

    test("should limit the chunk size to MAX_OUTGOING_MESSAGES_CHUNK_SIZE", async () => {
      // This test verifies chunking works when socket is already ready
      const { peer, mockWebSocket } = setup();

      mockWebSocket.send.mockImplementation((value: string) => {
        mockWebSocket.bufferedAmount += value.length;
      });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };
      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      // Fill up the buffer
      while (mockWebSocket.bufferedAmount < BUFFER_LIMIT) {
        peer.outgoing.push(message1);
      }

      mockWebSocket.send.mockClear();

      void peer.outgoing.push(message2);

      expect(mockWebSocket.send).not.toHaveBeenCalled();

      // Reset the buffer, make it look like we have sent the messages
      mockWebSocket.bufferedAmount = 0;

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });
    });

    test("should send accumulated messages before a large message", async () => {
      const { peer, mockWebSocket } = setup();

      mockWebSocket.bufferedAmount = BUFFER_LIMIT + 1;

      const smallMessage: SyncMessage = {
        action: "known",
        id: "co_z_small",
        header: false,
        sessions: {},
      };
      const largeMessage: SyncMessage = {
        action: "known",
        id: "co_z_large",
        header: false,
        sessions: {
          // Add a large payload to exceed MAX_OUTGOING_MESSAGES_CHUNK_BYTES
          payload: "x".repeat(MAX_OUTGOING_MESSAGES_CHUNK_BYTES),
        } as CojsonInternalTypes.CoValueKnownState["sessions"],
      };

      void peer.outgoing.push(smallMessage);
      void peer.outgoing.push(largeMessage);

      mockWebSocket.bufferedAmount = 0;

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(2);
      });

      expect(mockWebSocket.send).toHaveBeenCalledTimes(2);
      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        1,
        JSON.stringify(smallMessage),
      );
      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        2,
        JSON.stringify(largeMessage),
      );
    });

    test("should wait for the buffer to be under BUFFER_LIMIT before sending more messages", async () => {
      vi.useFakeTimers();
      const { peer, mockWebSocket } = setup();

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };
      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      // Start with buffer full so messages go through the queue
      mockWebSocket.bufferedAmount = BUFFER_LIMIT + 1;

      void peer.outgoing.push(message1);
      void peer.outgoing.push(message2);

      await vi.advanceTimersByTimeAsync(0);

      // No messages sent yet because buffer is full
      expect(mockWebSocket.send).not.toHaveBeenCalled();

      // Clear the buffer
      mockWebSocket.bufferedAmount = 0;

      await vi.advanceTimersByTimeAsync(BUFFER_LIMIT_POLLING_INTERVAL + 1);

      // Both messages are batched together
      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message1, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );

      vi.useRealTimers();
    });
  });

  describe("batchingByDefault = false", () => {
    test("should not batch outgoing messages", async () => {
      const { peer, mockWebSocket } = setup({ batchingByDefault: false });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      void peer.outgoing.push(message1);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(1);
      });

      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(2);
      });

      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        1,
        JSON.stringify(message1),
      );
      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        2,
        JSON.stringify(message2),
      );
    });

    test("should start batching outgoing messages when receiving a batched message", async () => {
      const { peer, mockWebSocket, triggerEvent } = setup({
        batchingByDefault: false,
        initialReadyState: 0,
      });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      triggerEvent(
        "message",
        new MessageEvent("message", {
          data: Array.from({ length: 5 }, () => message1)
            .map((msg) => JSON.stringify(msg))
            .join("\n"),
        }),
      );

      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      void peer.outgoing.push(message1);
      void peer.outgoing.push(message2);

      // Simulate socket becoming ready
      mockWebSocket.readyState = 1;
      triggerEvent("open");

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message1, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );
    });

    test("should not start batching outgoing messages when receiving non-batched message", async () => {
      const { peer, mockWebSocket, triggerEvent } = setup({
        batchingByDefault: false,
      });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      triggerEvent(
        "message",
        new MessageEvent("message", {
          data: JSON.stringify(message1),
        }),
      );

      const message2: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: 6,
      };

      void peer.outgoing.push(message1);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(1);
      });

      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(2);
      });

      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        1,
        JSON.stringify(message1),
      );
      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        2,
        JSON.stringify(message2),
      );
    });
  });

  describe("telemetry", () => {
    afterEach(() => {
      tearDownTestMetricReader();
    });

    test("should initialize to 0 when creating a websocket peer", async () => {
      const metricReader = createTestMetricReader();
      setup({
        meta: { test: "test" },
      });

      const measuredIngress = await metricReader.getMetricValue(
        "jazz.usage.ingress",
        {
          test: "test",
        },
      );
      expect(measuredIngress).toBe(0);
    });

    test("should correctly measure incoming ingress", async () => {
      const metricReader = createTestMetricReader();
      const { triggerEvent } = setup({
        meta: { label: "value" },
      });

      const encryptedChanges = "Hello, world!";
      triggerEvent(
        "message",
        new MessageEvent("message", {
          data: JSON.stringify({
            action: "content",
            new: {
              someSessionId: {
                after: 0,
                newTransactions: [
                  {
                    privacy: "private" as const,
                    madeAt: 0,
                    keyUsed: "key_zkey" as const,
                    encryptedChanges,
                  },
                ],
              },
            },
          }),
        }),
      );

      expect(
        await metricReader.getMetricValue("jazz.usage.ingress", {
          label: "value",
        }),
      ).toBe(encryptedChanges.length);

      const trustingChanges = "Jazz is great!";
      triggerEvent(
        "message",
        new MessageEvent("message", {
          data: JSON.stringify({
            action: "content",
            new: {
              someSessionId: {
                newTransactions: [
                  {
                    privacy: "trusting",
                    changes: trustingChanges,
                  },
                ],
              },
            },
          }),
        }),
      );

      expect(
        await metricReader.getMetricValue("jazz.usage.ingress", {
          label: "value",
        }),
      ).toBe(encryptedChanges.length + trustingChanges.length);
    });

    test("should drain the outgoing queue on websocket close so pulled equals pushed", async () => {
      const metricReader = createTestMetricReader();
      const { peer, triggerEvent } = setup({ initialReadyState: 0 });

      const high: SyncMessage = {
        action: "content",
        id: "co_zhigh",
        new: {},
        priority: CO_VALUE_PRIORITY.HIGH,
      };
      const medium: SyncMessage = {
        action: "content",
        id: "co_zmedium",
        new: {},
        priority: CO_VALUE_PRIORITY.MEDIUM,
      };
      const low: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: CO_VALUE_PRIORITY.LOW,
      };

      peer.outgoing.push(high);
      peer.outgoing.push(medium);
      peer.outgoing.push(low);

      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pushed", {
          priority: CO_VALUE_PRIORITY.HIGH,
          peerRole: "client",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pushed", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          peerRole: "client",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pushed", {
          priority: CO_VALUE_PRIORITY.LOW,
          peerRole: "client",
        }),
      ).toBe(1);

      // First message is already pulled by processQueue (waiting for socket open),
      // so pulled count for that priority is already 1
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pulled", {
          priority: CO_VALUE_PRIORITY.HIGH,
          peerRole: "client",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          peerRole: "client",
        }),
      ).toBe(0);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pulled", {
          priority: CO_VALUE_PRIORITY.LOW,
          peerRole: "client",
        }),
      ).toBe(0);

      triggerEvent("close");

      // After close, drain() is called which pulls all remaining messages
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pulled", {
          priority: CO_VALUE_PRIORITY.HIGH,
          peerRole: "client",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pulled", {
          priority: CO_VALUE_PRIORITY.MEDIUM,
          peerRole: "client",
        }),
      ).toBe(1);
      expect(
        await metricReader.getMetricValue("jazz.messagequeue.outgoing.pulled", {
          priority: CO_VALUE_PRIORITY.LOW,
          peerRole: "client",
        }),
      ).toBe(1);
    });
  });
});

// biome-ignore lint/suspicious/noConfusingVoidType: Test helper
function waitFor(callback: () => boolean | void) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = () => {
      try {
        return { ok: callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(() => {
      const { ok, error } = checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}

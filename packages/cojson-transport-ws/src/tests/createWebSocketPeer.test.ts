import type { CojsonInternalTypes, SyncMessage } from "cojson";
import { cojsonInternals } from "cojson";
import { describe, expect, type Mocked, test, vi } from "vitest";
import { MAX_OUTGOING_MESSAGES_CHUNK_BYTES } from "../BatchedOutgoingMessages.js";
import {
  type CreateWebSocketPeerOpts,
  createWebSocketPeer,
} from "../createWebSocketPeer.js";
import type { AnyWebSocket } from "../types.js";
import { BUFFER_LIMIT, BUFFER_LIMIT_POLLING_INTERVAL } from "../utils.js";

const { CO_VALUE_PRIORITY } = cojsonInternals;

function setup(opts: Partial<CreateWebSocketPeerOpts> = {}) {
  const listeners = new Map<string, (event: MessageEvent) => void>();

  const mockWebSocket = {
    readyState: 1,
    addEventListener: vi.fn().mockImplementation((type, listener) => {
      listeners.set(type, listener);
    }),
    removeEventListener: vi.fn().mockImplementation((type) => {
      listeners.delete(type);
    }),
    close: vi.fn(),
    send: vi.fn(),
  } as unknown as Mocked<AnyWebSocket>;

  const peer = createWebSocketPeer({
    id: "test-peer",
    websocket: mockWebSocket,
    role: "client",
    batchingByDefault: true,
    ...opts,
  });

  return { mockWebSocket, peer, listeners };
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
    const { listeners, peer } = setup();

    const onMessageSpy = vi.fn();
    peer.incoming.onMessage(onMessageSpy);

    const closeHandler = listeners.get("close");

    closeHandler?.(new MessageEvent("close"));

    expect(onMessageSpy).toHaveBeenCalledWith("Disconnected");
  });

  test("should handle ping timeout", async () => {
    vi.useFakeTimers();
    const { listeners, peer } = setup();

    const onMessageSpy = vi.fn();

    peer.incoming.onMessage(onMessageSpy);

    const messageHandler = listeners.get("message");

    messageHandler?.(new MessageEvent("message", { data: "{}" }));

    await vi.advanceTimersByTimeAsync(10_000);

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
    const { listeners } = setup({ onSuccess });

    const messageHandler = listeners.get("message");
    const message: SyncMessage = {
      action: "known",
      id: "co_ztest",
      header: false,
      sessions: {},
    };

    // First message should trigger onSuccess
    messageHandler?.(
      new MessageEvent("message", { data: JSON.stringify(message) }),
    );
    expect(onSuccess).toHaveBeenCalledTimes(1);

    // Subsequent messages should not trigger onSuccess again
    messageHandler?.(
      new MessageEvent("message", { data: JSON.stringify(message) }),
    );
    expect(onSuccess).toHaveBeenCalledTimes(1);
  });

  describe("batchingByDefault = true", () => {
    test("should batch outgoing messages", async () => {
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
      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message1, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );
    });

    test("should sort outgoing messages by priority", async () => {
      const { peer, mockWebSocket } = setup();

      mockWebSocket.send.mockImplementation(() => {
        mockWebSocket.readyState = 0;
      });

      const message1: SyncMessage = {
        action: "content",
        id: "co_zlow",
        new: {},
        priority: CO_VALUE_PRIORITY.LOW,
      };

      const message2: SyncMessage = {
        action: "content",
        id: "co_zhigh",
        new: {},
        priority: CO_VALUE_PRIORITY.HIGH,
      };

      void peer.outgoing.push(message1);
      void peer.outgoing.push(message2);
      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message2, message2, message1]
          .map((msg) => JSON.stringify(msg))
          .join("\n"),
      );
    });

    test("should send all the pending messages when the websocket is closed", async () => {
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
      void peer.outgoing.push(message2);

      peer.outgoing.close();

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message1, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );
    });

    test("should limit the chunk size to MAX_OUTGOING_MESSAGES_CHUNK_SIZE", async () => {
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

      const stream: SyncMessage[] = [];

      while (
        serializeMessages(stream.concat(message1)).length <
        MAX_OUTGOING_MESSAGES_CHUNK_BYTES
      ) {
        stream.push(message1);
        void peer.outgoing.push(message1);
      }

      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalledTimes(2);
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        serializeMessages(stream),
      );

      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        2,
        JSON.stringify(message2),
      );
    });

    test("should send accumulated messages before a large message", async () => {
      const { peer, mockWebSocket } = setup();

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

      mockWebSocket.send.mockImplementation(() => {
        mockWebSocket.bufferedAmount = BUFFER_LIMIT + 1;
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

      const stream: SyncMessage[] = [];

      while (
        serializeMessages(stream.concat(message1)).length <
        MAX_OUTGOING_MESSAGES_CHUNK_BYTES
      ) {
        stream.push(message1);
        void peer.outgoing.push(message1);
      }

      void peer.outgoing.push(message2);

      await vi.advanceTimersByTimeAsync(100);

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        serializeMessages(stream),
      );

      mockWebSocket.bufferedAmount = 0;

      await vi.advanceTimersByTimeAsync(BUFFER_LIMIT_POLLING_INTERVAL + 1);

      expect(mockWebSocket.send).toHaveBeenNthCalledWith(
        2,
        JSON.stringify(message2),
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
      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
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

    test("should start batching outgoing messages when reiceving a batched message", async () => {
      const { peer, mockWebSocket, listeners } = setup({
        batchingByDefault: false,
      });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      const messageHandler = listeners.get("message");

      messageHandler?.(
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

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
      });

      expect(mockWebSocket.send).toHaveBeenCalledWith(
        [message1, message2].map((msg) => JSON.stringify(msg)).join("\n"),
      );
    });

    test("should not start batching outgoing messages when reiceving non-batched message", async () => {
      const { peer, mockWebSocket, listeners } = setup({
        batchingByDefault: false,
      });

      const message1: SyncMessage = {
        action: "known",
        id: "co_ztest",
        header: false,
        sessions: {},
      };

      const messageHandler = listeners.get("message");

      messageHandler?.(
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
      void peer.outgoing.push(message2);

      await waitFor(() => {
        expect(mockWebSocket.send).toHaveBeenCalled();
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

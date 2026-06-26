import {
  BrowserWasmAbiSmokeClient,
  type DbHandle,
  type TransportHandle,
} from "./abi-smoke-worker-client.js";
import {
  bytesFromWebSocketMessage,
  connectWebSocket,
  decodeFrameBatch,
  encodeFrameBatch,
  webSocketConstructor,
  websocketTransportUrl,
  type MinimalWebSocket,
  type WebSocketConstructor,
} from "./websocket-transport.js";

export type WebSocketTransportStats = {
  sentFrames: number;
  receivedFrames: number;
  inboundTicks: number;
  watchWakes: number;
};

export type BrowserWebSocketTransportSync = {
  readonly url: string;
  readonly transport: TransportHandle;
  readonly stats: WebSocketTransportStats;
  close(): Promise<void>;
  flush(): Promise<void>;
};

export type OpenBrowserWebSocketTransportOptions = {
  url: string;
  client: BrowserWasmAbiSmokeClient;
  db: DbHandle;
  identity: Uint8Array;
  tickMs?: number;
  WebSocket?: WebSocketConstructor;
};

export async function openBrowserWebSocketTransport(
  options: OpenBrowserWebSocketTransportOptions,
): Promise<BrowserWebSocketTransportSync> {
  const WebSocketCtor =
    options.WebSocket ??
    webSocketConstructor("global WebSocket is unavailable; run in a browser or pass a constructor");
  const url = websocketTransportUrl(options.url, options.identity);
  const socket = await connectWebSocket(WebSocketCtor, url);
  const transport = await options.client.connectTransport(options.db, "upstream", new Uint8Array());
  const stats: WebSocketTransportStats = {
    sentFrames: 0,
    receivedFrames: 0,
    inboundTicks: 0,
    watchWakes: 0,
  };

  let closed = false;
  let flushing: Promise<void> | undefined;
  let sync: BrowserWebSocketTransportSync;
  const timer = setInterval(() => {
    void sync.flush();
  }, options.tickMs ?? 10);

  sync = {
    url,
    transport,
    stats,
    async close() {
      if (closed) return;
      closed = true;
      clearInterval(timer);
      await flushing;
      await options.client.transportClose(transport);
      socket.close();
    },
    async flush() {
      if (closed || socket.readyState !== 1) return;
      flushing ??= flushOnce(options.client, options.db, transport, socket, stats).finally(() => {
        flushing = undefined;
      });
      await flushing;
    },
  };

  socket.addEventListener("message", (event) => {
    void bytesFromWebSocketMessage(event.data).then(async (batch) => {
      if (closed) return;
      const frames = decodeFrameBatch(batch);
      for (const frame of frames) {
        await options.client.transportSendWireFrame(transport, frame);
      }
      stats.receivedFrames += frames.length;
      const tickStats = await options.client.transportTick(transport);
      if (tickStats) {
        stats.inboundTicks += 1;
        stats.watchWakes += Number(tickStats.watch_wakes ?? 0);
      }
      await sync.flush();
    });
  });

  await sync.flush();
  return sync;
}

async function flushOnce(
  client: BrowserWasmAbiSmokeClient,
  db: DbHandle,
  transport: TransportHandle,
  socket: MinimalWebSocket,
  stats: WebSocketTransportStats,
): Promise<void> {
  await client.transportTick(transport);
  const frames = await client.transportRecvWireFrames(transport, {
    max_frames: 16,
    max_bytes: 1024 * 1024,
  });
  if (frames.length > 0) {
    socket.send(encodeFrameBatch(frames));
    stats.sentFrames += frames.length;
  }
}

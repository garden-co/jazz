import { createServer, connect, type Socket } from "node:net";

/**
 * Test-client delivery gates matching jazz-testkit's Rust TransportControl.
 * Blocking buffers traffic without disconnecting; unblock delivers it in order.
 * New connections inherit the gates. Traffic already delivered cannot be recalled.
 */
export interface TransportControl {
  /** Use this URL for the controlled client's serverUrl, not other clients. */
  url: string;
  /** Stop delivery in both directions. */
  block(): void;
  /** Stop server-to-client delivery only, allowing writes to reach the server. */
  blockInbound(): void;
  /** Resume both directions, including buffered traffic. */
  unblock(): void;
  /**
   * The server goes away: close every connection and refuse new ones, as a
   * stopped backend does, until {@link TransportControl.up}.
   */
  down(): void;
  /**
   * The server is back, at `serverUrl` if given (a restarted backend on a
   * new port behind the same address the client knows).
   */
  up(serverUrl?: string): void;
  stop(): Promise<void>;
}

/**
 * TCP-backed implementation shared by NAPI, RN and browser tests.
 * Blocking preserves live sockets.
 * Use with local HTTP test servers; all TCP bytes, including handshakes, are gated.
 */
export async function createTransportControl(serverUrl: string): Promise<TransportControl> {
  const upstream = new URL(serverUrl);
  const portOf = (url: URL) => Number(url.port || (url.protocol === "https:" ? 443 : 80));
  let upstreamAddress = { port: portOf(upstream), hostname: upstream.hostname };
  let isDown = false;
  const sockets = new Set<Socket>();
  const queued: Array<{ destination: Socket; chunk: Buffer }> = [];
  let inboundBlocked = false;
  let outboundBlocked = false;

  const server = createServer((client) => {
    if (isDown) {
      client.destroy();
      return;
    }
    const target = connect(upstreamAddress.port, upstreamAddress.hostname);
    sockets.add(client);
    sockets.add(target);
    const close = () => {
      client.destroy();
      target.destroy();
      sockets.delete(client);
      sockets.delete(target);
    };
    client.on("data", (chunk) => {
      if (outboundBlocked) queued.push({ destination: target, chunk });
      else target.write(chunk);
    });
    target.on("data", (chunk) => {
      if (inboundBlocked) queued.push({ destination: client, chunk });
      else client.write(chunk);
    });
    client.on("close", close);
    target.on("close", close);
    client.on("error", close);
    target.on("error", close);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("transport control did not bind a TCP port");
  }

  return {
    // Keep the upstream's path, e.g. an app's `/jazz` endpoint.
    url: `http://127.0.0.1:${address.port}${upstream.pathname.replace(/\/$/, "")}`,
    block() {
      inboundBlocked = true;
      outboundBlocked = true;
    },
    blockInbound() {
      inboundBlocked = true;
    },
    unblock() {
      for (const { destination, chunk } of queued.splice(0)) {
        if (!destination.destroyed) destination.write(chunk);
      }
      inboundBlocked = false;
      outboundBlocked = false;
    },
    down() {
      isDown = true;
      queued.length = 0;
      for (const socket of sockets) socket.destroy();
      sockets.clear();
    },
    up(serverUrl?: string) {
      if (serverUrl) {
        const next = new URL(serverUrl);
        upstreamAddress = { port: portOf(next), hostname: next.hostname };
      }
      isDown = false;
    },
    async stop() {
      queued.length = 0;
      for (const socket of sockets) socket.destroy();
      sockets.clear();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

import { createServer, connect, type Socket } from "node:net";

/**
 * A transparent TCP proxy in front of a test Jazz server that can hold back
 * everything the server sends while keeping every connection open.
 *
 * While held, the client's link stays live (nothing closes, nothing errors)
 * but no server answer reaches it, which is the only way to observe a
 * first-load wait ending at its deadline rather than on an answer or a
 * link loss. Releasing delivers the held bytes in order.
 */
export interface HoldingProxy {
  /** `http://127.0.0.1:<port>`: use it as the client's `serverUrl`. */
  url: string;
  hold(): void;
  release(): void;
  stop(): Promise<void>;
}

export async function startHoldingProxy(serverUrl: string): Promise<HoldingProxy> {
  const upstream = new URL(serverUrl);
  const upstreamPort = Number(upstream.port || (upstream.protocol === "https:" ? 443 : 80));
  const sockets = new Set<Socket>();
  const heldChunks: Array<{ client: Socket; chunk: Buffer }> = [];
  let held = false;

  const server = createServer((client) => {
    const target = connect(upstreamPort, upstream.hostname);
    sockets.add(client);
    sockets.add(target);
    const close = () => {
      client.destroy();
      target.destroy();
      sockets.delete(client);
      sockets.delete(target);
    };
    client.on("data", (chunk) => target.write(chunk));
    target.on("data", (chunk) => {
      if (held) heldChunks.push({ client, chunk });
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
    throw new Error("holding proxy did not bind a TCP port");
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    hold() {
      held = true;
    },
    release() {
      held = false;
      for (const { client, chunk } of heldChunks.splice(0)) {
        if (!client.destroyed) client.write(chunk);
      }
    },
    async stop() {
      for (const socket of sockets) socket.destroy();
      sockets.clear();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

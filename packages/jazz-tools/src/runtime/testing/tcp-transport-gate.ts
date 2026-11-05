import { connect, createServer, type Socket } from "node:net";

export interface TransportGate {
  url: string;
  block(): void;
  unblock(): void;
  close(): Promise<void>;
}

/** Cut both live and future connections without changing the client's online state. */
export async function transportGate(targetUrl: string): Promise<TransportGate> {
  const target = new URL(targetUrl);
  const sockets = new Set<Socket>();
  let blocked = false;
  const server = createServer((socket) => {
    if (blocked) {
      socket.destroy();
      return;
    }
    const upstream = connect({ host: target.hostname, port: Number(target.port) });
    for (const stream of [socket, upstream]) {
      sockets.add(stream);
      stream.on("error", () => {
        socket.destroy();
        upstream.destroy();
      });
      stream.on("close", () => sockets.delete(stream));
    }
    socket.pipe(upstream).pipe(socket);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing transport gate address");
  return {
    url: `http://127.0.0.1:${address.port}`,
    block() {
      blocked = true;
      for (const socket of sockets) socket.destroy();
    },
    unblock() {
      blocked = false;
    },
    async close() {
      blocked = true;
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}

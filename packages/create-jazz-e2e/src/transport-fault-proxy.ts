import { createServer as createHttpServer } from "node:http";
import { createServer, connect, type Socket } from "node:net";
import { randomBytes } from "node:crypto";

/** Test-only byte proxy: works for transports owned by either a page or a worker. */
export async function startTransportFaultProxy(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl);
  if (upstream.protocol !== "ws:" && upstream.protocol !== "http:") {
    throw new Error("Transport fault proxy requires a plaintext local test server");
  }
  const sockets = new Set<Socket>();
  let blocked = false;
  let droppedBytes = 0;
  let receivedBytes = 0;
  const proxy = createServer((client) => {
    const remote = connect(Number(upstream.port), upstream.hostname);
    sockets.add(client);
    sockets.add(remote);
    for (const socket of [client, remote]) {
      socket.on("error", () => {
        client.destroy();
        remote.destroy();
      });
      socket.on("close", () => {
        sockets.delete(socket);
        client.destroy();
        remote.destroy();
      });
    }
    client.on("data", (data: Buffer) => {
      if (blocked) {
        droppedBytes += data.length;
        client.destroy();
        remote.destroy();
      } else if (!remote.write(data)) {
        client.pause();
      }
    });
    remote.on("drain", () => client.resume());
    remote.on("data", (data: Buffer) => {
      receivedBytes += data.length;
    });
    remote.pipe(client);
  });
  const token = randomBytes(24).toString("hex");
  const control = createHttpServer((request, response) => {
    if (request.url !== `/${token}`) {
      response.writeHead(404).end();
      return;
    }
    if (request.method === "POST") {
      blocked = true;
      droppedBytes = 0;
    } else if (request.method === "DELETE") {
      blocked = false;
    } else if (request.method !== "GET") {
      response.writeHead(405).end();
      return;
    }
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ blocked, droppedBytes, receivedBytes }));
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  await new Promise<void>((resolve) => control.listen(0, "127.0.0.1", resolve));
  const proxyAddress = proxy.address();
  const controlAddress = control.address();
  if (
    !proxyAddress ||
    typeof proxyAddress === "string" ||
    !controlAddress ||
    typeof controlAddress === "string"
  ) {
    throw new Error("Expected loopback proxy addresses");
  }
  const proxyUrl = new URL(upstream);
  proxyUrl.hostname = "127.0.0.1";
  proxyUrl.port = String(proxyAddress.port);
  return {
    url: proxyUrl.href,
    controlUrl: `http://127.0.0.1:${controlAddress.port}/${token}`,
    async stop() {
      for (const socket of sockets) socket.destroy();
      await Promise.all(
        [proxy, control].map(
          (server) =>
            new Promise<void>((resolve, reject) => {
              server.close((error) => (error ? reject(error) : resolve()));
            }),
        ),
      );
    },
  };
}

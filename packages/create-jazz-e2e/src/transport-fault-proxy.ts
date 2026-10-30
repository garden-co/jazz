import { createServer as createHttpServer } from "node:http";
import { createServer, connect, type Socket, type Server } from "node:net";
import { randomBytes } from "node:crypto";

async function listenOnLoopback(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
}

/** Test-only byte proxy: works for transports owned by either a page or a worker. */
export async function startTransportFaultProxy(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl);
  if (
    (upstream.protocol !== "ws:" && upstream.protocol !== "http:") ||
    !["127.0.0.1", "localhost", "[::1]"].includes(upstream.hostname)
  ) {
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
  const stop = async () => {
    for (const socket of sockets) socket.destroy();
    await Promise.all(
      [proxy, control].map(
        (server) =>
          new Promise<void>((resolve, reject) => {
            if (!server.listening) return resolve();
            server.close((error) => (error ? reject(error) : resolve()));
          }),
      ),
    );
  };
  try {
    await listenOnLoopback(proxy);
    await listenOnLoopback(control);
  } catch (error) {
    await stop();
    throw error;
  }
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
    stop,
  };
}

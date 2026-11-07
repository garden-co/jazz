import { once } from "node:events";
import { connect, createServer, type Socket } from "node:net";
import { setTimeout } from "node:timers/promises";
import { expect, it } from "vitest";
import { createTransportControl } from "./transport-control.js";

async function fixture() {
  const peers = new Set<Socket>();
  let received = "";
  const server = createServer((socket) => {
    peers.add(socket);
    socket.on("close", () => peers.delete(socket));
    socket.on("data", (chunk) => {
      received += chunk.toString();
      socket.write(chunk);
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test server port");
  const control = await createTransportControl(`http://127.0.0.1:${address.port}`);
  const clients: Socket[] = [];
  return {
    control,
    received: () => received,
    async client() {
      const socket = connect(Number(new URL(control.url).port), "127.0.0.1");
      clients.push(socket);
      let responses = "";
      socket.on("data", (chunk) => (responses += chunk.toString()));
      await once(socket, "connect");
      return { socket, responses: () => responses };
    },
    async stop() {
      for (const client of clients) client.destroy();
      await control.stop();
      for (const peer of peers) peer.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

it("blocks inbound delivery while writes reach the server, then resumes in order", async () => {
  const f = await fixture();
  try {
    const { socket, responses } = await f.client();
    socket.write("ready;");
    await expect.poll(responses).toBe("ready;");
    f.control.blockInbound();
    socket.write("first;");
    socket.write("second;");
    await expect.poll(f.received).toBe("ready;first;second;");
    // Give the echoed bytes time to reach the gate before checking withholding.
    await setTimeout(50);
    expect(responses()).toBe("ready;");
    expect(socket.destroyed).toBe(false);
    f.control.unblock();
    socket.write("third;");
    await expect.poll(responses).toBe("ready;first;second;third;");
  } finally {
    await f.stop();
  }
});

it("blocks both directions on existing and new connections until unblocked", async () => {
  const f = await fixture();
  try {
    const first = await f.client();
    first.socket.write("ready;");
    await expect.poll(first.responses).toBe("ready;");
    f.control.block();
    const second = await f.client();
    first.socket.write("first;");
    second.socket.write("second;");
    await setTimeout(50);
    expect(f.received()).toBe("ready;");
    expect(first.responses()).toBe("ready;");
    expect(second.responses()).toBe("");
    f.control.unblock();
    await expect.poll(first.responses).toBe("ready;first;");
    await expect.poll(second.responses).toBe("second;");
  } finally {
    await f.stop();
  }
});

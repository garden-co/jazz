import assert from "node:assert/strict";
import { test } from "node:test";
import { connect, createServer } from "node:net";
import { once } from "node:events";
import { startTransportFaultProxy } from "./transport-fault-proxy.js";

test("transport proxy forwards, drops actual bytes while armed, and reconnects", async () => {
  const upstream = createServer((socket) => socket.pipe(socket));
  upstream.listen(0, "127.0.0.1");
  await once(upstream, "listening");
  const address = upstream.address();
  assert(address && typeof address !== "string");
  const proxy = await startTransportFaultProxy(`http://127.0.0.1:${address.port}`);
  const proxyUrl = new URL(proxy.url);
  const sockets: ReturnType<typeof connect>[] = [];
  const open = async () => {
    const socket = connect(Number(proxyUrl.port), proxyUrl.hostname);
    sockets.push(socket);
    await once(socket, "connect");
    return socket;
  };
  const state = async (method = "GET") => {
    const response = await fetch(proxy.controlUrl, { method });
    assert.equal(response.status, 200);
    return (await response.json()) as { droppedBytes: number; receivedBytes: number };
  };
  try {
    const socket = await open();
    socket.write("before");
    assert.equal(String((await once(socket, "data"))[0]), "before");
    await state("POST");
    const closed = once(socket, "close", { signal: AbortSignal.timeout(5_000) });
    socket.write("dropped");
    await closed;
    assert.equal((await state()).droppedBytes, 7);
    const beforeReconnect = await state("DELETE");
    const reconnected = await open();
    reconnected.write("after");
    assert.equal(String((await once(reconnected, "data"))[0]), "after");
    assert((await state()).receivedBytes > beforeReconnect.receivedBytes);
  } finally {
    for (const socket of sockets) socket.destroy();
    await proxy.stop();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  }
});

import assert from "node:assert/strict";
import test from "node:test";
import type { UniversalWebSocket } from "rivetkit";
import { jazzRequest, jazzSocket } from "./rivet-socket.js";

class Socket extends EventTarget {
  binaryType = "blob";
  bufferedAmount = 0;
  sent: unknown[] = [];
  closed: { code?: number; reason?: string } | undefined;
  send(data: unknown) {
    this.sent.push(data);
  }
  close(code?: number, reason?: string) {
    this.closed = { code, reason };
    this.dispatchEvent(new Event("close"));
  }
  receive(data: string | ArrayBuffer) {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
  adapt() {
    return jazzSocket(this as unknown as UniversalWebSocket);
  }
}

test("normalizes only Rivet's request prefix, preserving auth and query", () => {
  const request = new Request("https://actor/request/jazz/apps/a/ws?trace=1", {
    headers: { authorization: "Bearer fixture" },
  });
  const normalized = jazzRequest(request);
  assert.equal(normalized.url, "https://actor/jazz/apps/a/ws?trace=1");
  assert.equal(normalized.headers.get("authorization"), "Bearer fixture");
  assert.equal(jazzRequest(new Request("https://actor/requested")).url, "https://actor/requested");
});

test("preserves message order across pauses and drains one at a time", () => {
  const socket = new Socket();
  const adapter = socket.adapt();
  const received: unknown[] = [];
  adapter.on("message", (data) => {
    received.push(data);
    adapter.pause!();
  });
  socket.receive("first");
  socket.receive("second");
  socket.receive("third");
  assert.deepEqual(received, ["first"]);
  adapter.resume!();
  assert.deepEqual(received, ["first", "second"]);
  adapter.resume!();
  assert.deepEqual(received, ["first", "second", "third"]);
});

test("closes on byte or message-count overflow instead of accumulating work", () => {
  for (const oversized of [false, true]) {
    const socket = new Socket();
    const adapter = socket.adapt();
    adapter.pause!();
    if (oversized) socket.receive(new ArrayBuffer(2 * 1024 * 1024 + 1));
    else for (let i = 0; i < 65; i++) socket.receive("");
    assert.equal(socket.closed?.code, 1009);
  }
});

test("bounds outgoing buffers and forwards errors and close", () => {
  const socket = new Socket();
  const adapter = socket.adapt();
  let closed = false;
  let error: unknown;
  adapter.on("close", () => {
    closed = true;
  });
  adapter.on("error", (event) => {
    error = event;
  });
  const event = new Event("error");
  socket.dispatchEvent(event);
  assert.equal(error, event);
  socket.bufferedAmount = 2 * 1024 * 1024;
  adapter.send(new Uint8Array([1]));
  assert.equal(socket.closed?.code, 1013);
  assert.equal(closed, true);
  assert.equal(socket.sent.length, 0);
});

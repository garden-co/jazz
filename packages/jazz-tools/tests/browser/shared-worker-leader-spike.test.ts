// packages/jazz-tools/tests/browser/shared-worker-leader-spike.test.ts
import { describe, expect, it } from "vitest";

describe("shared-worker-leader spike: MessagePort transfer", () => {
  it("tab → SharedWorker → tab port transfer delivers a round-trip message", async () => {
    if (typeof SharedWorker === "undefined") return;

    const worker = new SharedWorker(
      new URL("./fixtures/leader-spike.shared-worker.js", import.meta.url),
      { type: "module", name: `spike-${Math.random().toString(36).slice(2)}` },
    );

    const portPromise = new Promise<MessagePort>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("timeout waiting for port")), 5000);
      worker.port.onmessage = (event: MessageEvent) => {
        if (event.data?.t === "PEER_PORT" && event.ports[0]) {
          clearTimeout(timeout);
          resolve(event.ports[0]);
        }
      };
    });
    worker.port.start();
    worker.port.postMessage({ t: "HELLO", tabId: "spike-tab" });

    const port = await portPromise;
    const echoPromise = new Promise<unknown>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("timeout waiting for echo")), 5000);
      port.onmessage = (event: MessageEvent) => {
        clearTimeout(timeout);
        resolve(event.data);
      };
    });
    port.start();
    port.postMessage({ ping: "spike" });

    const echoed = await echoPromise;
    expect(echoed).toEqual({ pong: "spike" });
  }, 20000);
});

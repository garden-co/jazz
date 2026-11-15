import { MessageChannel } from "node:worker_threads";
import { expect, test } from "vitest";
import { attachInspectorCacheRuntime } from "./inspector-cache-runtime.js";

test("schema views sharing an Inspector port correlate their own replies", async () => {
  const channel = new MessageChannel();
  const binding = {
    appId: "synthetic",
    physicalDbName: "synthetic",
    authSessionKey: "synthetic",
    storageOwner: "synthetic",
  };
  const left = attachInspectorCacheRuntime(
    { discard() {} } as never,
    channel.port1 as never,
    binding,
    {},
  );
  const right = attachInspectorCacheRuntime(
    { discard() {} } as never,
    channel.port1 as never,
    binding,
    {},
  );
  channel.port2.on("message", (message) =>
    channel.port2.postMessage({
      type: "inspector-query-result",
      id: message.id,
      value: message.query,
    }),
  );
  channel.port1.start();
  try {
    const results = await Promise.all([
      left.query("first", null, "local-only"),
      right.query("second", null, "local-only"),
    ]);
    expect(results).toEqual(["first", "second"]);
  } finally {
    left.discard();
    right.discard();
    channel.port1.close();
    channel.port2.close();
  }
});

test("stays open on a refusal the worker held at attach, and closes on a new one", async () => {
  const channel = new MessageChannel();
  const binding = {
    appId: "synthetic",
    physicalDbName: "synthetic",
    authSessionKey: "synthetic",
    storageOwner: "synthetic",
  };
  const runtime = attachInspectorCacheRuntime(
    { discard() {} } as never,
    channel.port1 as never,
    binding,
    {},
  );
  const fate = (authFailure: string | null) => ({
    type: "link-fate",
    fate: { state: "connected", link: { state: "connected" }, failure: null, authFailure },
  });
  const delivered = () => new Promise((resolve) => setTimeout(resolve, 10));
  channel.port1.start();
  try {
    channel.port2.postMessage(fate("expired"));
    channel.port2.postMessage(fate("expired"));
    await delivered();
    channel.port2.on("message", (message) =>
      channel.port2.postMessage({ type: "inspector-query-result", id: message.id, value: "ok" }),
    );
    await expect(runtime.query("q", null, "local-only")).resolves.toBe("ok");

    channel.port2.postMessage(fate(null));
    channel.port2.postMessage(fate("invalid"));
    await delivered();
    await expect(runtime.query("q", null, "local-only")).rejects.toThrow();
  } finally {
    runtime.discard();
    channel.port1.close();
    channel.port2.close();
  }
});

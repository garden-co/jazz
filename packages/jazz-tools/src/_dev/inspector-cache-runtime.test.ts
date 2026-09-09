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
      left.query("first", null, "local", '{"propagation":"local-only"}'),
      right.query("second", null, "local", '{"propagation":"local-only"}'),
    ]);
    expect(results).toEqual(["first", "second"]);
  } finally {
    left.discard();
    right.discard();
    channel.port1.close();
    channel.port2.close();
  }
});

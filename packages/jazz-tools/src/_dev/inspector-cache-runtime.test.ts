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

test("Inspector refuses staged mutation capabilities before runtime or source work", async () => {
  const channel = new MessageChannel();
  let calls = 0;
  let pulls = 0;
  const target = {
    discard() {},
    async stageStreamingMutation(...args: unknown[]) {
      calls++;
      for await (const _chunk of args[4] as AsyncIterable<Uint8Array>) calls++;
      return { id: "opaque-fixture", attach() {}, abort: () => true };
    },
  };
  const inspector = attachInspectorCacheRuntime(
    target as never,
    channel.port1 as never,
    {
      appId: "synthetic",
      physicalDbName: "synthetic",
      authSessionKey: "synthetic",
      storageOwner: "synthetic",
    },
    {},
  );
  const source = async function* () {
    pulls++;
    yield new Uint8Array([1]);
  };
  try {
    await expect(
      Promise.resolve().then(() =>
        inspector.stageStreamingMutation!(
          "insert",
          "files",
          {},
          "payload",
          source(),
          undefined,
          undefined,
          "client-accepted-evidence",
        ),
      ),
    ).rejects.toThrow("This mutation is not supported by the Inspector");
    expect(calls).toBe(0);
    expect(pulls).toBe(0);
  } finally {
    inspector.discard();
    channel.port1.close();
    channel.port2.close();
  }
});
